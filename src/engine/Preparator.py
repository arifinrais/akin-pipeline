#!/usr/bin/env python3
import sys, time, logging, json, csv, traceback
from typing import Pattern #, os, logging
import requests as req
import pandas as pd
from engine.Engine import Engine
from engine.EngineHelper import GenerateFileName, BytesToDataFrame, CreateCSVLine
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

class Preparator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_TRANSFORM']
        self.bucket = self.settings['MINIO_BUCKET_TRANSFORMED']
        self.previous_bucket = self.settings['MINIO_BUCKET_AGGREGATED']
        self.column_names = ['no_permohonan','no_sertifikat','status','tanggal_dimulai','tanggal_berakhir','daftar_kelas','alamat']

    def _setup_spark(self):
        try:
            self.spark_conf = SparkConf()
            self.spark_conf.setAll([
                ('spark.master', self.settings['SPARK_MASTER']),# <--- this host must be resolvable by the driver in this case pyspark (whatever it is located, same server or remote) in our case the IP of server
                ('spark.app.name', self.settings['SPARK_APP_NAME'])
            ])
            return True
        except:
            print(sys.exc_info()) #for debugging
            return False

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
            mapped_lines, unmapped_lines = self._spark_splitting(cleaned_lines)
            geocoded_lines = self._rq_geocoding(unmapped_lines) #maybe spark also can do it
            for line in geocoded_lines:
                mapped_lines.append(line)
            #save the result file to minio
            self._save_lines_to_minio_in_csv(mapped_lines, self.bucket, dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    def _spark_cleaning(self, dataframe, col_name="_c6"):
        REGEXP_LIST = [
            #remove long spaces
            ("\s+", " "),
            #remove telephone number
            ("(?i)\s(telp?\.|telp\s).*$", ""),
            ("(?i)\(?perubahan\salamat.*$", ""),
            #remove to be noted remarks
            ("(?i)\(\s?u\.?p\.?.*\)", ""),
            ("(?i)\s\(?\s?u\.?p\.?\s[a-z].*$", ""),
            #remove general remarks
            ("\(.*\)", ""),
            #fix commas position
            ("(,\s,)+", ","),
            (",+", ","),
            #remove postal address
            ("(?i)[\s\*\(][Aa]lamat\s[SsKk2].*$", ""),
            #remove other remaining clutter
            ("(?i)\(perubahan\salamat\)", ","),
            ("¿+", ""),
            ("#.*#", ""),
            ("#+", "")]
        COUNTRY_LIST = [
            'india',
            'u.s.a',
            'san diego, ca',
            'korea',
            'china',
            'california',
            'thailand',
            'singapore',
            'switzerland',
            'japan',
            'indiana',
            'united states',
            'germany',
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
        spark_session = SparkSession.builder.config(conf=self.spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        df = spark_session.createDataFrame(dataframe)
        df = df.filter(df[col_name].rlike('^\s*$')==False) #filter empty addresses
        for regexp in REGEXP_LIST: #transforming addresses format for quality
            rgxpattern, replacement = regexp
            df = df.withColumn(col_name, regexp_replace(col(col_name), rgxpattern, replacement))
        for country in COUNTRY_LIST: #filter foreign addresses
            df = df.filter(df[col_name].rlike('(?i)^.*'+country+'.*$')==False)
        lines=[]
        for row in df.collect():
            lines.append(CreateCSVLine(row,lineterminator=''))
        spark_session.stop()
        return lines

    def _spark_splitting(self, dataframe, col_name="_c6"):
        #split address berdasarkan comma
        #cek length list address
        #initiate mapped_list=[]
        #if length >3:
            #cek pake fuzzywuzzy elemen -3,-2,-1 -> permutasi 3: kota-prov-negara, 2: kota-negara|kota-prov|prov-negara, 1: kota|prov|negara
        #elif length==3:
            #cek elemen -2,-1 -> permutasi 2: kota-negara|kota-prov|prov-negara, 1: kota|prov|negara
        #else
            #cek elemen -1 (cek out of index ga) -> permutasi 1: kota-negara|kota-prov|prov-negara, 1: kota|prov|negara
        #assign highest kota, provinsi, negara
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
        setup_spark = self._setup_spark()
        logging.info("Preparator Engine Successfully Started") if  setup_redis and setup_minio and setup_spark else logging.warning("Problem in Starting Preparator Engine")
        
        self._transform() #for debugging
        return
        while True:
            self._transform()
            time.sleep(self.settings['SLEEP_TIME'])
        