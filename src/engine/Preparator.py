#!/usr/bin/env python3
import sys, time, json, csv, traceback #, os, logging
import requests as req
#from os import stat
#from tenacity import retry
#from jsonschema import validate
#from abc import ABC, abstractmethod
from engine.Engine import Engine
from datetime import datetime
from io import BytesIO, StringIO
from copy import deepcopy
from redis import Redis
from rejson import Client, Path
from rq import Connection as RedisQueueConnection
from rq.queue import Queue
from rq.job import Job 
from minio import Minio
from minio.error import S3Error
from pyspark.conf import SparkConf
from pyspark import sql
from pymongo import MongoClient

class Preparator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_TRANSFORM']

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
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._transform_file(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    def _transform_file(self, dimension, year):
        bucket_name=self.settings['MINIO_AGGREGATED_IDENTIFIER']
        file_name=self._generate_file_name(bucket_name, dimension, year, '.csv')
        try:
            #load the file from minio
            resp = self.minio_client.get_object(bucket_name, file_name)
            lines=[]            
            try:
                resp = self.minio_client.get_object(bucket_name, file_name)
                resp_utf = resp.decode('utf-8')
                lines = self._transform_in_spark(resp_utf, dimension, year)   
            finally:
                resp.close()
                resp.release_conn()
            #save the result file to minio
            self._save_lines_to_minio_in_csv(lines, self.settings['MINIO_TRANSFORMED_IDENTIFIER'], dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    def _transform_in_spark(self, resp, dimension, year):
        #submit cleaning, pattern-matching(?), geocoding, encoding job to SPARK
        resp_stream = StringIO(resp)
        spark_session = sql.SparkSession.builder.config(conf=self.spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_reader = spark_session.read
        spark_stream_reader = spark_session.readStream
        spark_context.setLogLevel("WARN")
        #######
        ip_dataframe  = spark_session.createDataFrame(resp_stream.split("\n"))
                                            
        myGDF = ip_dataframe.select('*').groupBy('col1')
        ip_dataframe.createOrReplaceTempView('ip_dataframe_as_sqltable')
        print(ip_dataframe.collect())
        myGDF.sum().show()
        #
        spark_session.stop(); #quit()

        return 1
        #https://github.com/bitnami/bitnami-docker-spark/issues/18
        
    def start(self):
        self._setup_redis_conn()
        self._setup_minio_client()
        self._setup_spark()
        while True:
            self._transform()
            time.sleep(self.settings['SLEEP_TIME'])