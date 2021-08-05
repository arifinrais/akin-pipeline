#!/usr/bin/env python3
import sys, time, json, csv, traceback #, os, logging
import requests as req
#from os import stat
#from tenacity import retry
#from jsonschema import validate
#from abc import ABC, abstractmethod
from engine.Engine import Engine
from engine.EngineHelper import GenerateFileName, BytesToLines
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

class Analytics(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_ANALYZE']
        self.previous_bucket = self.settings['MINIO_BUCKET_TRANSFORMED']
        self.result_folder = self.settings['MINIO_RESULT_FOLDER']
        self.collections = self.settings['MONGO_COLLECTIONS'] #viz, anl

    def _analyze(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._analyze_file(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    def _analyze_file(self, dimension, year):
        file_name=GenerateFileName(self.previous_bucket, dimension, year, 'csv', temp_folder=self.result_folder)
        try:
            line_list = self._fetch_and_parse(self.previous_bucket, file_name, 'csv')
            
            viz_schemes = self._translate_viz(line_list,dimension, year)
            self._save_to_mongodb(viz_schemes, dimension, year)

            anl_schemes = self._complexity_analysis(line_list, dimension, year)
            self._save_to_mongodb(anl_schemes, dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    def _translate_viz(line_list, dimension, year):
        #translate
        #save to mongodb
        pass

    def _translate_anl(line_list, dimension, year):
        pass

    def _complexity_analysis(self, resp, dimension, year):
        pass

    def _save_to_mongodb(self, resp, analyses, year):
        complexity_collection = self.mongo_database[self.settings['MONGODB_COLLECTION_REGIONAL_PATENT']]
        None

    def start(self):
        self._setup_redis_conn()
        self._setup_minio_client()
        self._setup_mongo_client()
        while True:
            self._analyze()
            time.sleep(self.settings['SLEEP_TIME'])

