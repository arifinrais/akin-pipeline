#!/usr/bin/env python3
import sys, time, logging, json, csv, traceback #, os, logging
import requests as req
import pandas as pd
from engine.Engine import Engine
from engine.EngineHelper import GenerateFileName, BytesToDataFrame
from datetime import datetime
from io import BytesIO, StringIO
from copy import deepcopy
from redis import Redis
from minio import Minio
from minio.error import S3Error
from pyspark import sql
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
#from pyspark.sql.types import *

class Preparator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_TRANSFORM']
        self.bucket = self.settings['MINIO_BUCKET_TRANSFORMED']
        self.previous_bucket = self.settings['MINIO_BUCKET_AGGREGATED']

    def _setup_spark(self):
        self.spark_conf = SparkConf()
        self.spark_conf.setAll([
            ('spark.master', self.settings['SPARK_MASTER']),# <--- this host must be resolvable by the driver in this case pyspark (whatever it is located, same server or remote) in our case the IP of server
            ('spark.app.name', self.settings['SPARK_APP_NAME']),
            ('spark.submit.deployMode', self.settings['SPARK_SUBMIT_DEPLOY_MODE']),
            ('spark.ui.showConsoleProgress', self.settings['SPARK_UI_SHOW_CONSOLE_PROGRESS']),
            ('spark.eventLog.enabled', self.settings['SPARK_EVENT_LOG_ENABLED']),
            ('spark.logConf', self.settings['SAPRK_LOG_CONF_']),
            ('spark.driver.bindAddress', self.settings['SPARK_DRIVER_BIND_ADDRESS']),# <--- this host is the IP where pyspark will bind the service running the driver (normally 0.0.0.0)
            ('spark.driver.host', self.settings['SPARK_DRIVER_HOST']),# <--- this host is the resolvable IP for the host that is running the driver and it must be reachable by the master and master must be able to reach it (in our case the IP of the container where we are running pyspark
        ])

    def _transform(self):
        logging.debug('Acquiring Lock for Transformation Jobs...')
        key, dimension, year = self._redis_update_stat_before(self.job)
        logging.debug('Transforming Records...')
        success, errormsg = self._transform_file(dimension, year)
        logging.debug('Updating Job Status...')
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    def _transform_file(self, dimension, year):
        file_name=GenerateFileName(self.previous_bucket, dimension, year, '.csv')
        try:
            #load the file from minio
            lines=[]
            try:
                resp = self.minio_client.get_object(self.previous_bucket, file_name)
                #nanti bisa dirapihin masalah fieldsnya
                df = BytesToDataFrame(resp.data, ['no_permohonan','no_sertifikat','status','tanggal_dimulai','tanggal_berakhir','daftar_kelas','alamat'])
                self._transform_in_spark(df, dimension, year)
            except S3Error: raise S3Error
            except:
                print(sys.exc_info())
            finally:
                resp.close()
                resp.release_conn()
            #save the result file to minio
            self._save_lines_to_minio_in_csv(lines, self.settings['MINIO_BUCKET_TRANSFORMED'], dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    def _transform_in_spark(self, dataframe, dimension, year):
        def _clean_address_field(s):
            return s.split(", ")
        #submit cleaning, pattern-matching(?), geocoding, encoding job to SPARK
        
        try:
            spark_session = sql.SparkSession.builder.config(conf=self.spark_conf).getOrCreate()
            spark_context = spark_session.sparkContext
            spark_reader = spark_session.read
            spark_stream_reader = spark_session.readStream
            spark_context.setLogLevel("WARN")
            #######
            df = spark_session.createDataFrame(dataframe)
            df.select(transform("alamat", _clean_address_field)).show(5)
        except:
            print(sys.exc_info())
                                            
        #myGDF = ip_dataframe.select('*').groupBy('col1')
        #ip_dataframe.createOrReplaceTempView('ip_dataframe_as_sqltable')
        #print(ip_dataframe.collect())
        #myGDF.sum().show()
        #
        finally: 
            spark_session.stop(); #quit()

        return 1
        #https://github.com/bitnami/bitnami-docker-spark/issues/18
        
    def start(self):
        setup_redis = self._setup_redis_conn()
        setup_minio = self._setup_minio_client()
        setup_spark = self._setup_spark()
        logging.info("Preparator Engine Successfully Started") if  setup_redis and setup_minio and setup_spark else logging.warning("Problem in Starting Preparator Engine")
        while True:
            self._transform()
            return #try spark
            time.sleep(self.settings['SLEEP_TIME'])
        