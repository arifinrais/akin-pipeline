#!/usr/bin/env python3
import sys, time, logging, json, csv, traceback
from typing import Pattern #, os, logging
import requests as req
import pandas as pd
from engine.Engine import Engine
from engine.EngineHelper import GenerateFileName, BytesToDataFrame, CreateCSVLine, LinesToDataFrame
from datetime import datetime
from io import BytesIO, StringIO
from copy import deepcopy
from redis import Redis
from minio import Minio
from minio.error import S3Error
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from fuzzywuzzy import fuzz

class Preparator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_TRANSFORM']
        self.bucket = self.settings['MINIO_BUCKET_TRANSFORMED']
        self.previous_bucket = self.settings['MINIO_BUCKET_AGGREGATED']
        self.column_names = ['no_permohonan','no_sertifikat','status','tanggal_dimulai','tanggal_berakhir','daftar_kelas','alamat']

    def _setup_spark(self, app_name=None):
        try:
            _app_name = app_name if app_name else self.settings['SPARK_APP_NAME']
            spark_conf = SparkConf()
            spark_conf.setAll([
                ('spark.master', self.settings['SPARK_MASTER']),# <--- this host must be resolvable by the driver in this case pyspark (whatever it is located, same server or remote) in our case the IP of server
                ('spark.app.name', _app_name),
                ('spark.submit.deployMode', self.settings['SPARK_SUBMIT_DEPLOY_MODE']),
                ('spark.ui.showConsoleProgress', self.settings['SPARK_UI_SHOW_CONSOLE_PROGRESS']),
                ('spark.eventLog.enabled', self.settings['SPARK_EVENT_LOG_ENABLED']),
                ('spark.logConf', self.settings['SAPRK_LOG_CONF_']),
                ('spark.driver.bindAddress', self.settings['SPARK_DRIVER_BIND_ADDRESS']),# <--- this host is the IP where pyspark will bind the service running the driver (normally 0.0.0.0)
                ('spark.driver.host', self.settings['SPARK_DRIVER_HOST']),# <--- this host is the resolvable IP for the host that is running the driver and it must be reachable by the master and master must be able to reach it (in our case the IP of the container where we are running pyspark
            ])
            return spark_conf
        except:
            print(sys.exc_info()) #for debugging
            return None

    def _transform(self):
        #logging.debug('Acquiring Lock for Transformation Jobs...')
        #key, dimension, year = self._redis_update_stat_before(self.job)
        #logging.debug('Transforming Records...')
        #success, errormsg = self._transform_file(dimension, year)
        #logging.debug('Updating Job Status...')
        #self._redis_update_stat_after(key, self.job, success, errormsg)
        success, errormsg = self._transform_file('ptn', 2018) #for debugging
        print(success, errormsg)

    def _transform_file(self, dimension, year):
        file_name=GenerateFileName(self.previous_bucket, dimension, year, '.csv')
        try:
            df=None
            try:
                resp = self.minio_client.get_object(self.previous_bucket, file_name)
                #nanti bisa dirapihin masalah fieldsnya
                df = BytesToDataFrame(resp.data, self.column_names)
            except S3Error: raise S3Error
            finally:
                resp.close()
                resp.release_conn()
            cleaned_lines = self._spark_cleaning(df, self.column_names[-1])
            mapped_lines, unmapped_lines = self._spark_splitting(LinesToDataFrame(cleaned_lines))
            geocoded_lines = self._rq_geocoding(unmapped_lines) #maybe spark also can do it
            for line in geocoded_lines:
                mapped_lines.append(line)
            #save the result file to minio
            self._save_lines_to_minio_in_csv(mapped_lines, self.bucket, dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg


    '''
        Notes: Usually the address format is as follows,
            <Street Address><City>(<General Remarks>?)(<Province>?)(<Country>?)(<Postal Code>?)
            (<To Be Noted>|<Telephone Number>|<Fax Number>)?(<Country>?)(<Previous Address>|<Postal Address>)?
        Clean from the end!
    '''
    def _spark_cleaning(self, dataframe, col_name="_c6"):
        REGEXP_LIST = [
            #remove long spaces
            ("\s+", " "),
            #remove postal/correspondence address
            ("(?i)[\s\*\(][Aa]lamat\s[SsKk2].*$", ""),
            #remove previous address
            ("(?i)\(?perubahan\salamat.*$", ""),
            #remove telephone number
            (("(?i)\s(telp?\.|telp\s).*indonesia\s+?$", ", INDONESIA")),
            ("(?i)\s(telp?\.|telp\s).*$", ""),
            #remove to be noted remarks
            ("(?i)\(\s?u\.?p\.?.*\)", ""),
            ("(?i)\s\(?\s?u\.?p\.?\s[a-z].*indonesia\s+?$", ", INDONESIA"),
            ("(?i)\s\(?\s?u\.?p\.?\s[a-z].*$", ""),
            #remove general remarks non-postal code
            ("\((?!\d{5}).*\)", ""),
            #fix commas position
            (",\s,(\s,)*", ", "),
            (",+", ","),
            #remove other remaining clutter
            ("¿+", ""),
            ("#+", ""),
            ("·+", ""),
            (";", ""),
            #remove long spaces again
            ("\s+", " ")]
        COUNTRY_LIST = [
            'india','gujarat',
            'u.s.a','california','indiana','san diego, ca','united states', 
            'korea', 
            'china', 
            'thailand', 
            'singapore', 
            'switzerland', 
            'japan', 'tokyo',
            'germany', 'erlangen',
            'sweden', 
            'netherlands', 
            'italy', 
            'belgium', 
            'philippines', 
            'malaysia', 
            'france', 
            'norway', 
            'united kingdom', 
            'finland'] 
        spark_conf = self._setup_spark(app_name='data_cleaning')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        df = spark_session.createDataFrame(dataframe)
        df = df.filter(df[col_name].rlike('^\s*$')==False) #filter empty addresses
        for regexp in REGEXP_LIST: #transforming addresses format for quality
            rgxpattern, replacement = regexp
            df = df.withColumn(col_name, regexp_replace(col(col_name), rgxpattern, replacement))
        for country in COUNTRY_LIST: #filter foreign addresses
            df = df.filter(df[col_name].rlike('(?i)^.*'+country+'(?![a-z]).*$')==False)
        lines=[]
        for row in df.collect():
            lines.append(CreateCSVLine(row,lineterminator=''))
        spark_session.stop()
        return lines

    def _spark_splitting(self, dataframe, col_name="_c6"):
        def map_postal(s,std_file):
            for rec in std_file:
                p_range=rec['postal_range'].strip().split('-')
                if int(s)>=int(p_range[0]) and int(s)<=int(p_range[1]):
                    return rec['city']+';'+rec['province'] #format can be changed
            return 'POSTAL_MAPPING_ERROR'
        
        def udf_map_postal(std_file):
            return udf(lambda x: map_postal(x,std_file), StringType())
        
        def map_pattern(s,std_file):
            def fuzz_rating(city, std_file):
                maxRtg=0; maxLoc=None
                city=city.strip().lower()
                fuzz_calc=lambda x,y: (fuzz.ratio(x,y)+fuzz.partial_ratio(x,y)+fuzz.token_sort_ratio(x,y)+fuzz.token_set_ratio(x,y))/4
                for rec in std_file:
                    _city = rec['city'].lower()
                    rating1 = fuzz_calc(city,_city)
                    _city = rec['city_official'].lower()
                    rating2 = fuzz_calc(city,_city)
                    if rec['city_alias']:
                        _city = rec['city_alias'].lower()
                        rating3 = fuzz_calc(city,_city)
                    else: rating3=0
                    rating = rating1 if rating1>rating2 and rating1>rating3 else rating2 if rating2>rating3 else rating3
                    if rating==100: return 100, rec['city']+";"+rec['province'] #format can be changed
                    if rating>maxRtg:
                        maxRtg=rating
                        maxLoc=rec['city']+";"+rec['province'] #format can be changed
                return maxRtg, maxLoc
            OFFSET=88
            address_params = s.split(',')
            max_rating, max_region = 0, ''
            num_of_params = len(address_params) 
            max_param = 3 if num_of_params>3 else 2 if num_of_params>2 else 1 if num_of_params>1 else 0
            if max_param:
                for i in range(max_param):
                    r_loc, loc = fuzz_rating(address_params[-i-1],std_file)
                    if r_loc==100: return loc
                    if r_loc>max_rating:
                        max_rating=r_loc
                        max_region=loc
            else:
                return s
            return max_region if max_rating>=OFFSET else s
            
        def udf_map_pattern(std_file):
            return udf(lambda x: map_pattern(x,std_file),StringType())

        std_file = 'tar dikirim std_file dari parameter sebelumnya dari minio' #JANLUP!!!!!!!

        spark_conf = self._setup_spark(app_name='data_splitting')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        df = spark_session.createDataFrame(dataframe)

        #split based on postal code
        regex_address_postal = '(\s|-|,|\.|\()\d{5}($|\s|,|\)|\.|-)'; regex_postal = '\d{5}'
        df_postal_mapped = df.filter(df[col_name].rlike(regex_address_postal)==True).\
                            withColumn(col_name, regexp_extract(col_name, regex_address_postal, 0)).\
                            withColumn(col_name, regexp_extract(col_name, regex_postal, 0)).\
                            withColumn(col_name,  udf_map_postal(std_file)(col(col_name))) #mapping
        df = df.filter(df[col_name].rlike(regex_address_postal)==False).\
                withColumn(col_name,  udf_map_pattern(std_file)(col(col_name))) #mapping
        
        #split based on pattern-matched
        df_pattern_mapped = df.filter(df[col_name].rlike('some_pattern')==True) #sesuain format apa kasih flag aja?
        df_to_geocode = df.filter(df[col_name].rlike('some_pattern')==False) #sesuain format

        mapped_lines=[];unmapped_lines=[]
        for row in df_postal_mapped.collect():
            mapped_lines.append(CreateCSVLine(row,lineterminator=''))
        for row in df_pattern_mapped.collect():
            mapped_lines.append(CreateCSVLine(row,lineterminator=''))
        for row in df_to_geocode.collect():
            unmapped_lines.append(CreateCSVLine(row,lineterminator=''))
        spark_session.stop()
        return mapped_lines, unmapped_lines

    def _spark_pattern_matching(self):
        pass

    def _rq_geocoding(self, lines):
        #set request list
        #loop to enqueue
        #wait for job
        #return result
        pass
        
    def start(self):
        setup_redis = self._setup_redis_conn()
        setup_minio = self._setup_minio_client()
        logging.info("Preparator Engine Successfully Started") if  setup_redis and setup_minio and setup_spark else logging.warning("Problem in Starting Preparator Engine")
        
        self._transform() #for debugging
        return
        while True:
            self._transform()
            time.sleep(self.settings['SLEEP_TIME'])
        