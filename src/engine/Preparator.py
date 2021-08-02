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
            df = LinesToDataFrame(cleaned_lines)
            mapped_lines, unmapped_lines = self._spark_splitting(df)
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

    CITY_LIST=[
        'Bandung', #bandung dan kota bandung samain
        'Jakarta Utara',
        'Medan',
        'Surabaya',
        'Yogyakarta'
    ]
    PROVINCE_LIST = [
        'Jawa Barat',
        'Jawa Timur',
        'DKI Jakarta',
        'Sumatera Utara'
    ]
    COUNTRY_LIST = ['Indonesia']

    def _spark_splitting(self, dataframe, col_name="_c6"):
        def _location_fuzz_rating(loc, list_of_loc):
            maxVal=0; maxLoc=None
            llw=loc.lower()
            for _loc in list_of_loc:
                _llw= _loc.lower()
                loc_ratio = (fuzz.ratio(llw,_llw)+fuzz.partial_ratio(llw,_llw)+\
                    fuzz.token_sort_ratio(llw,_llw)+fuzz.token_set_ratio(llw,_llw))/4
                if loc_ratio>maxVal:
                    maxVal=loc_ratio
                    maxLoc=_loc
            return maxVal, maxLoc

        def _address_splitting(s):
            address_params=s.split(',')
            num_of_params=len(address_params)
            if num_of_params>3:
                city=address_params[-3]; province = address_params[-2]; country=address_params[-1]
                maxCity, maxCityRate = _location_fuzz_rating(city, self.CITY_LIST)
                if maxCityRate > 90: 
                    #map address
                    #append address to mapped_list
                    pass
        
                
                
            pass
        
        def _map_postal_code(s): #masukin ke udf ntar
            pass

        spark_conf = self._setup_spark(app_name='data_cleaning')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        df = spark_session.createDataFrame(dataframe)
        regex_address_postal = '(\s|-|,|\.|\()\d{5}($|\s|,|\)|\.|-)'
        regex_postal = '\d{5}'
        df_postal_coded = df.filter(df[col_name].rlike(regex_address_postal)==True)
        df_postal_coded = df_postal_coded.withColumn(col_name, regexp_extract(col_name, regex_postal, 0))
        #regexp extract -> map postal coded udf
        #jangan lupa cek locationsnya juga -> split(',') fuzz 100% country/province/city if not, split(' ') fuzz 100% country/province/city

        df = df.filter(df[col_name].rlike(regex_address_postal)==False)
        #split address berdasarkan comma
        #cek length list address
        #initiate mapped_list=[]
        #if length >3:
            #cek pake fuzzywuzzy elemen -3,-2,-1 -> permutasi 3: kota-prov-negara, 2: kota-negara|kota-prov|prov-negara, 1: kota|prov|negara
            #bisa displit lagi pake spasi
        #elif length==3:
            #cek elemen -2,-1 -> permutasi 2: kota-negara|kota-prov|prov-negara, 1: kota|prov|negara
            #bisa displit lagi pake spasi
        #else
            #cek elemen -1 (cek out of index ga) -> permutasi 1: kota-negara|kota-prov|prov-negara, 1: kota|prov|negara
            #bisa displit lagi pake spasi
        #assign highest kota, provinsi, negara
        #if kota >=90% -> mapped -> mapped_list.append
        #if negara>=80% -> if cek kode pos -> mapped -> mapped_list.append
        #if kota+provinsi/2 >=70% if cek kode pos -> mapped -> mapped_list.append
        
        #if not assigned and ada kode pos
            #split by commas, and then space
            #if kota >=90% -> mapped -> mapped_list.append
            #if negara>=80% -> if cek kode pos -> mapped -> mapped_list.append
            #if kota+provinsi/2 >=70% if cek kode pos -> mapped -> mapped_list.append
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
        