#!/usr/bin/env python3
import sys, time, json, csv, traceback #, os, logging
from engine.Engine import Engine
import requests as req
#from os import stat
#from tenacity import retry
#from jsonschema import validate
#from abc import ABC, abstractmethod
from datetime import datetime
from redis import Redis
from rejson import Client, Path
from rq import Connection as RedisQueueConnection
from rq.queue import Queue
from rq.job import Job 
from minio import Minio
from minio.error import S3Error
from io import BytesIO, StringIO
from copy import deepcopy
from pyspark.conf import SparkConf
from pyspark import sql
from pymongo import MongoClient

class Analytics(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_ANALYZE']

    def _setup_mongo_client(self):
        self.mongo_client = MongoClient(self.settings['MONGODB_URI'])
        self.mongo_database = self.mongo_client[self.settings['MONGODB_DATABASE']]
    
    def _analyze(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._analyze_file(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    def _analyze_file(self, dimension, year):
        bucket_name=self.settings['MINIO_TRANSFORMED_IDENTIFIER']
        file_name=self._generate_file_name(bucket_name, dimension, year, '.csv')
        try:
            try:
                resp = self.minio_client.get_object(bucket_name, file_name)
                resp_utf = resp.decode('utf-8')
                analyses = self._complexity_analysis(resp_utf, dimension, year)
            finally:
                resp.close()
                resp.release_conn()
            self._save_to_mongodb(resp_utf, analyses, dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    def _complexity_analysis(self, resp, dimension, year):
        None

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

