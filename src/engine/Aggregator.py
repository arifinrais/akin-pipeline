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

class Aggregator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_AGGREGATE']
    
    def _aggregate(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._aggregate_records(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    def _aggregate_records(self, dimension, year):
        bucket_name=self.settings['MINIO_INGESTED_IDENTIFIER']
        try:
            #load the objects from minio
            filenames = self.minio_client.list_objects(bucket_name) #can add prefix or recursive
            parsed_lines = []
            #parse and aggregate
            for filename in filenames:
                #assuming list_objects return the name of the object
                try:
                    resp = self.minio_client.get_object(bucket_name, filename)
                    resp_utf = resp.decode('utf-8')
                    lines = self._parse_object(resp_utf, dimension, year)
                    for line in lines:
                        parsed_lines.append(deepcopy(line))
                finally:
                    resp.close()
                    resp.release_conn()
            unique_lines=self._uniquify(parsed_lines)
            self._save_lines_to_minio_in_csv(unique_lines, self.settings['MINIO_AGGREGATED_IDENTIFIER'], dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    def _parse_object(self, resp, dimension, year):
        lines = []
        if self._check_dimension_source('PDKI', dimension):
            records = resp.json()['hits']['hits']
            for record in records:
                id_application, id_certificate, status, date_begin, date_end = \
                    record['_source']['id'], \
                    record['_source']['nomor_sertifikat'], \
                    record['_source']['status_permohonan'], \
                    record['_source']['tanggal_dimulai_perlindungan'], \
                    record['_source']['tanggal_berakhir_perlindungan']
                classes = [i['ipc_full'] for i in record['_source']['ipc']]
                address = None; inventor_address = None; owner_address = None
                try:
                    owner_address = next(i['alamat_pemegang'] for i in record['_source']['owner'] if i['alamat_pemegang'] != '-')
                    inventor_address = next(i['alamat_inventor'] for i in record['_source']['inventor'] if i['alamat_inventor'] != '-')
                finally:
                    if inventor_address:
                        address = inventor_address
                    elif owner_address:
                        address = owner_address
                lines.append(self._create_csv_line([id_application,id_certificate,status, date_begin, date_end, classes, address]))
        elif self._check_dimension_source('SINTA', dimension):
            #for every html (soup) object
                #parse and make a csv line with \t delimiter
                #append to lines
            None     
        return lines
    
    def _uniquify(self, lines):
        unique_lines=[]
        seen = set()
        for line in lines:
            if line in seen: continue
            seen.add(line)
            unique_lines.append(line)
        return unique_lines

    def start(self):
        self._setup_redis_conn()
        self._setup_minio_client()
        while True:
            self._aggregate()
            time.sleep(self.settings['SLEEP_TIME'])
