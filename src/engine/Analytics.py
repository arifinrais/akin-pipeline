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
        self.resources_bucket = self.settings['MINIO_BUCKET_RESOURCES']
        self.result_folder = self.settings['MINIO_RESULT_FOLDER']
        self.encoder_dictionary = self.settings['RES_FILES']['ANL_ENC_STD']
        self.collections = self.settings['MONGO_COLLECTIONS'] #viz, anl

    def _analyze(self):
        #key, dimension, year = self._redis_update_stat_before(self.job)
        #success, errormsg = self._analyze_file(dimension, year)
        #self._redis_update_stat_after(key, self.job, success, errormsg)
        success, errormsg = self._analyze_file('ptn', 2018)
        print(success, errormsg)

    
    def _analyze_file(self, dimension, year):
        file_name=GenerateFileName(self.previous_bucket, dimension, year, 'csv', temp_folder=self.result_folder)
        try:
            line_list = self._fetch_and_parse(self.previous_bucket, file_name, 'csv')

            ll_encoded = self._encode(line_list)
            #_class_enc, weight, _city, _province, _island, _dev_main, _dev_econ
            
            viz_schemes = self._translate_viz(ll_encoded,dimension, year)
            self._save_to_mongodb(viz_schemes, dimension, year)

            anl_schemes = self._complexity_analysis(ll_encoded, dimension, year)
            self._save_to_mongodb(anl_schemes, dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    def _encode(self, line_list, dimension, delimiter='\t', class_delimiter=';', decimal_places=2):
        std_file = self._fetch_and_parse(self.resources_bucket, self.encoder_dictionary, 'json')
        ll_encoded = []
        for line in line_list:
            _line = line.strip().split(delimiter)
            classes, city, province = _line[5], _line[7], _line[8]
            _city, _province, _island, _dev_main, _dev_econ = self._encode_region(city, province, std_file)
            _classes = classes.split(class_delimiter)
            if _island!=-1 or _dev_main!=-1:
                weight = round*((1/len(_classes)), decimal_places)
                for _class in _classes:
                    _class_enc = self._encode_class(_class, std_file['ipc_class2']) if dimension==self.settings['DIMENSION_PATENT']\
                        else self._encode_class(_class, std_file['ncl_class1']) #nanti add buat SINTA (pub)
                    if _class_enc!=-1:
                        ll_encoded.append([_class_enc, weight, _city, _province, _island, _dev_main, _dev_econ])
        return ll_encoded
    
    def _encode_region(self, city, province, std_file):
        _city, _province, _island, _dev_main, _dev_econ = -1, -1, -1, -1, -1
        for rec in std_file['city']:
            if city==rec['city']:
                _city=rec['id']
                _province=rec['parent_id']
                break
        for rec in std_file['province']:
            if _province==rec['id'] and province==rec['province']:
                _island=rec['island_id']
                _dev_econ=rec['parent_id']
                break
        for rec in std_file['dev_econ']:
            if _dev_econ==rec['id']:
                _dev_main=rec['parent_id']
                break
        return _city, _province, _island, _dev_main, _dev_econ

    def _encode_class(self, _class, std_class):
        __class=-1
        for rec in std_class:
            if _class==rec['class']:
                __class=rec['id']
                break
        return __class

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

