#!/usr/bin/env python3
import sys, time, logging, json, regex as re, csv, traceback
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
        self.resources_bucket = self.settings['MINIO_BUCKET_RESOURCES']
        self.standard_file_region = self.settings['FILE_REGION_STANDARD']
        self.standard_file_department = self.settings['FILE_DEPARTMENT_STANDARD']
        self.mapped_folder = 'tmp_mapped'
        self.unmapped_folder = 'tmp_unmapped'
        self.result_folder = 'result'
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
                #('spark.driver.bindAddress', self.settings['SPARK_DRIVER_BIND_ADDRESS']),# <--- this host is the IP where pyspark will bind the service running the driver (normally 0.0.0.0)
                #('spark.driver.host', self.settings['SPARK_DRIVER_HOST']),# <--- this host is the resolvable IP for the host that is running the driver and it must be reachable by the master and master must be able to reach it (in our case the IP of the container where we are running pyspark
            ])
            return spark_conf
        except:
            print(sys.exc_info()) #for debugging
            return None

    def _transform(self):
        #logging.debug('Acquiring Lock for Transformation Jobs...')
        #key, dimension, year = self._redis_update_stat_before(self.job)
        #logging.debug('Transforming Records...')
        #success, errormsg = self._transform_in_spark(dimension, year)
        #logging.debug('Do Geocoding...')
        #if success: success, errormsg = self._geocoding(dimension, year)
        #logging.debug('Updating Job Status...')
        #self._redis_update_stat_after(key, self.job, success, errormsg)
        success, errormsg = self._transform_in_spark('ptn', 2018) #for debugging
        #success, errormsg = self._geocoding('ptn', 2018) #for debugging
        print(success, errormsg)

    def _transform_in_spark(self, dimension, year):
        file_name=GenerateFileName(self.previous_bucket, dimension, year, 'csv')
        try:
            data_output = self._fetch_file_from_minio(self.previous_bucket, file_name)
            df = BytesToDataFrame(data_output, self.column_names) if data_output else None
            data_output = self._fetch_file_from_minio(self.resources_bucket, self.standard_file_region)
            std_file = json.load(BytesIO(data_output))
            print('mashuk')
            #cleaning the data
            df_cleaned = self._spark_cleaning(df, self.column_names[-1])
            print(df_cleaned)
            #splitting based on postal code and pattern-matching
            df_mapped_postal, df_unmapped = self._spark_splitting_postal(df_cleaned, std_file, self.column_names[-1])
            df_mapped_pattern, df_unmapped = self._spark_splitting_pattern(df_unmapped, std_file, self.column_names[-1])
            
            #save mapped and unmapped dataframes
            df_mapped = df_mapped_postal.append(df_mapped_pattern)
            mapped_lines = []
            for mapped_values in df_mapped.values.tolist():
                mapped_lines.append(CreateCSVLine(mapped_values))
            self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.mapped_folder)
            unmapped_lines = []
            for unmapped_values in df_unmapped.values.tolist():
                unmapped_lines.append(CreateCSVLine(unmapped_values))
            self._save_data_to_minio(unmapped_lines, self.bucket, dimension, year, temp_folder=self.unmapped_folder)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    '''
        Notes: Usually the address format is as follows,
            <Street Address><City>(<General Remarks>?)(<Province>?)(<Country>?)(<Postal Code>?)
            (<To Be Noted>|<Telephone Number>|<Fax Number>)?(<Country>?)(<Previous Address>|<Postal Address>)?
        Clean from the end!
        PREVIOUS VERSION
        for regexp in REGEXP_LIST: #transforming addresses format for quality
            rgxpattern, replacement = regexp
            df = df.withColumn(col_name, regexp_replace(col(col_name), rgxpattern, replacement))
        for country in COUNTRY_LIST: #filter foreign addresses
            df = df.filter(df[col_name].rlike('(?i)^.*'+country+'(?![a-z]).*$')==False)
        lines=[]
        for row in df.collect():
            lines.append(CreateCSVLine(row,lineterminator=''))
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
        #setup spark udfs
        def clean_address(s,regexp_list):
            for regex_pair in regexp_list:
                regexp, replacement = regex_pair
                s=re.sub(regexp,replacement,s)
            return s
        
        def udf_clean_address(regexp_list):
            return udf(lambda x: clean_address(x,regexp_list), StringType())

        def flag_foreign_address(s,country_list):
            for country in country_list:
                if country.lower() in s.lower(): return 'FOREIGN_ADDRESS'
            return s

        def udf_flag_foreign_address(regexp_list):
            return udf(lambda x: flag_foreign_address(x,regexp_list), StringType())

        #setup spark
        spark_conf = self._setup_spark(app_name='data_cleaning')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        df = spark_session.createDataFrame(dataframe)
        
        #cleaning address data
        df = df.filter(df[col_name].rlike('^\s*$')==False)\
            .withColumn(col_name, udf_flag_foreign_address(COUNTRY_LIST)(col(col_name))).cache() #so it can be easily accessed by filter
        df = df.filter(df[col_name].rlike('FOREIGN_ADDRESS')==False)\
            .withColumn(col_name, udf_clean_address(REGEXP_LIST)(col(col_name)))
        df_cleaned = df.toPandas()
        
        spark_session.stop()
        return df_cleaned

    def _spark_splitting_postal(self, dataframe, std_file, col_name="_c6"):
        #setup spark udfs
        def map_postal(s,std_file):
            for rec in std_file:
                p_range=rec['postal_range'].strip().split('-')
                if int(s)>=int(p_range[0]) and int(s)<=int(p_range[1]):
                    return rec['city']+';'+rec['province'] #format can be changed
            return 'POSTAL_MAPPING_ERROR'
        
        def udf_map_postal(std_file):
            return udf(lambda x: map_postal(x,std_file), StringType())

        #setup spark
        spark_conf = self._setup_spark(app_name='data_splitting_postal')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        df = spark_session.createDataFrame(dataframe)
        
        #split and map based on postal code
        regex_address_postal = '(\s|-|,|\.|\()\d{5}($|\s|,|\)|\.|-)'; regex_postal = '\d{5}'
        df_postal_mapped = df.filter(df[col_name].rlike(regex_address_postal)==True).\
                            withColumn(col_name, regexp_extract(col_name, regex_address_postal, 0)).\
                            withColumn(col_name, regexp_extract(col_name, regex_postal, 0)).\
                            withColumn(col_name, udf_map_postal(std_file)(col(col_name))) #mapping
        df_unmapped = df.filter(df[col_name].rlike(regex_address_postal)==False).toPandas()
        df_postal_mapped= df_postal_mapped.toPandas()
        
        spark_session.stop()
        return df_postal_mapped, df_unmapped

    def _spark_splitting_pattern(self, dataframe, std_file, col_name="_c6"):
        #setup spark udfs
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
            FUZZ_RATING_OFFSET=88
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
            return max_region if max_rating>=FUZZ_RATING_OFFSET else s
            
        def udf_map_pattern(std_file):
            return udf(lambda x: map_pattern(x,std_file),StringType())

        #setup spark
        spark_conf = self._setup_spark(app_name='data_splitting_pattern')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        df = spark_session.createDataFrame(dataframe)

        #map and split based on address pattern
        df = df.withColumn(col_name, udf_map_pattern(std_file)(col(col_name))) #mapping
        regex_address_pattern='\t.*\[ADDRESS_MATCHED\]'#sesuain format apa kasih flag aja?
        df_pattern_mapped = df.filter(df[col_name].rlike(regex_address_pattern)==True).toPandas() 
        df_unmapped = df.filter(df[col_name].rlike(regex_address_pattern)==False).toPandas()
        
        spark_session.stop()
        return df_pattern_mapped, df_unmapped

    def _geocoding(self, dimension, year):
        mapped_fname=GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.mapped_folder)
        try:
            #open unmapped dataset
            unmapped_fname=GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.unmapped_folder)
            data_output = self._fetch_file_from_minio(self.bucket, unmapped_fname)
            df = BytesToDataFrame(data_output, self.column_names) if data_output else None
            data_output = self._fetch_file_from_minio(self.resources_bucket, self.standard_file_region)
            std_file = json.load(BytesIO(data_output))
            
            #geocode unmapped data in spark
            #bisa _geocoding_in_rq atau _geocoding_bare
            df_geocoded = self._spark_geocoding(df, self.column_names[-1])
            _df_mapped = self._spark_mapping_gc(df_geocoded, std_file, self.column_names[-1])

            #open mapped dataset
            mapped_fname = GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.mapped_folder)
            data_output = self._fetch_file_from_minio(self.bucket, mapped_fname)
            df = BytesToDataFrame(data_output, self.column_names) if data_output else None

            #join and save mapped and geocoded data
            df_mapped = df.append(_df_mapped)
            mapped_lines = []
            for mapped_values in df_mapped.values.tolist():
                mapped_lines.append(CreateCSVLine(mapped_values))
            self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.result_folder) #buat bucket khusus?
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
        pass
        
    def _spark_geocoding(self, dataframe, col_name="_c6"):
        #setup spark udfs
        def geocode(s,_config):
            #config request sesuai APInya
            #hit api
            #parse response
            #return parsed response
            pass
        
        def udf_geocode(_config):
            return udf(lambda x: geocode(x,_config), StringType())
        
        #setup config
        _config={} #sesuain sama APInya

        #setup spark
        spark_conf = self._setup_spark(app_name='data_splitting_postal')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        
        #geocoding based on bare address
        df = spark_session.createDataFrame(dataframe)
        df_geocoded = df.withColumn(col_name, udf_geocode(_config)(col(col_name))).toPandas()
        
        spark_session.stop()
        return df_geocoded

    def _spark_mapping_gc(self, dataframe, std_file, col_name="_c6"):
        #setup spark udfs
        def map_geocode(s,std_file):
            fuzz_calc=lambda x,y: (fuzz.ratio(x,y)+fuzz.partial_ratio(x,y)+fuzz.token_sort_ratio(x,y)+fuzz.token_set_ratio(x,y))/4
            maxRtg, maxLoc = 0, None
            locs=s.strip().split('\t') #sesuain format hasil parsing geocode, misal <city>\t<province>
            fuzz_calc=lambda x,y: (fuzz.ratio(x,y)+fuzz.partial_ratio(x,y)+fuzz.token_sort_ratio(x,y)+fuzz.token_set_ratio(x,y))/4
            for rec in std_file:
                _prov = rec['province'].lower()
                _city = rec['city'].lower()
                rating = (fuzz_calc(locs[0],_city)+fuzz_calc(locs[1],_prov))/2
                if rating>maxRtg:
                    maxLoc=rec['city']+'\t'+rec['province']
            return maxLoc if maxLoc else 'GEOCODE_MAPPING_ERROR'
        
        def udf_map_geocode(std_file):
            return udf(lambda x: map_geocode(x,std_file), StringType())

        #setup spark
        spark_conf = self._setup_spark(app_name='data_splitting_postal')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        
        #mapping geocoded address
        df = spark_session.createDataFrame(dataframe)
        df_mapped = df.withColumn(col_name, udf_map_geocode(std_file)(col(col_name))).toPandas()
        
        spark_session.stop()
        return df_mapped

    def _geocoding_in_rq(self, dimension, year):
        #set request list
        #loop to enqueue
        #wait for job
        #return result
        pass
        
    def _geocoding_bare(self, dimension, year):
        pass

    def start(self):
        setup_redis = self._setup_redis_conn()
        setup_minio = self._setup_minio_client()
        logging.info("Preparator Engine Successfully Started") if  setup_redis and setup_minio else logging.warning("Problem in Starting Preparator Engine")    
        self._transform() #for debugging
        return
        while True:
            self._transform()
            time.sleep(self.settings['SLEEP_TIME'])
        