#!/usr/bin/env python3
import re
import sys, time, json, csv, traceback
from xml.etree.ElementTree import indent
from minio.datatypes import Bucket #, os, logging
import requests as req
from requests.api import request
#from os import stat
#from tenacity import retry
#from jsonschema import validate
#from abc import ABC, abstractmethod
from engine.Engine import Engine
from engine.EngineHelper import CreateCSVLine
from datetime import datetime
from io import BytesIO, StringIO
from copy import deepcopy
from redis import Redis
from rejson import Client, Path
from rq import Connection as RedisQueueConnection
from rq.queue import Queue
from rq.job import Job 
from minio import Minio
from minio.api import SelectRequest
from minio.select import InputSerialization, JSONInputSerialization, JSONOutputSerialization
from minio.error import S3Error
from pyspark.conf import SparkConf
from pyspark import sql
from pymongo import MongoClient

class Aggregator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_AGGREGATE']
        self.bucket = self.settings['MINIO_BUCKET_AGGREGATED']
        self.previous_bucket = self.settings['MINIO_BUCKET_INGESTED']
    
    def _aggregate(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._aggregate_records(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    def _aggregate_records(self, dimension, year):
        try:
            folder = dimension+'/'+str(year)+'/'
            obj_list = self.minio_client.list_objects(self.previous_bucket, prefix=folder, recursive=True) #can add prefix or recursive
            parsed_lines = []
            for obj in obj_list:
                if self._check_dimension_source('PDKI', dimension):
                    lines=self._parse_json(obj.object_name, dimension)
                    if len(lines): 
                        for line in lines: parsed_lines.append(line) 
                elif self._check_dimension_source('SINTA', dimension):
                    lines=self._parse_html(obj.object_name, dimension)
                    if len(lines): 
                        for line in lines: parsed_lines.append(line)
                else:
                    raise Exception('405: Parser Not Found')
            unique_lines=self._uniquify(parsed_lines)
            print('unique_lines length :',len(unique_lines))
            print('unique_lines :', unique_lines[0])
            self._save_lines_to_minio_in_csv(unique_lines, self.bucket, dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    def _parse_html(self, obj_name):
        pass

    def _parse_json(self, obj_name, dimension):
        try:
            resp = self.minio_client.get_object(self.previous_bucket, obj_name)
            json_obj = json.load(BytesIO(resp.data))
            lines = self._parse_object(json_obj, dimension)
        except S3Error: raise S3Error
        finally:
            resp.close()
            resp.release_conn()
            return lines

    def _parse_object(self, obj, dimension):
        lines = []
        records = obj['hits']
        counter=0 #buat ngecek aja
        if dimension==self.settings['DIMENSION_PATENT']:
            for record in records:
                id_application, id_certificate, status, date_begin, date_end = \
                    record['_source']['id'], \
                    record['_source']['nomor_sertifikat'], \
                    record['_source']['status_permohonan'], \
                    record['_source']['tanggal_dimulai_perlindungan'], \
                    record['_source']['tanggal_berakhir_perlindungan']
                if not id_certificate: continue #filter applied but not listed patent
                address = self._parse_address(dimension, record['_source']['owner'], record['_source']['inventor'])
                if not address: continue #patent can't be located or it's not an indonesian patent
                classes = self._parse_classes(record['_source']['ipc'])
                if not classes: continue
                lines.append(CreateCSVLine([id_application,id_certificate,status, date_begin, date_end, classes, address]))
                counter+=1
        elif dimension==self.settings['DIMENSION_TRADEMARK']:
            for record in records:
                id_application, id_certificate, status, date_begin, date_end, classes = \
                    record['_source']['id'], \
                    record['_source']['nomor_pendaftaran'], \
                    record['_source']['status_permohonan'], \
                    record['_source']['tanggal_dimulai_perlindungan'], \
                    record['_source']['tanggal_berakhir_perlindungan'], \
                    record['_source']['t_class']['class_no']
                if not id_certificate or not classes: continue #filter applied but not listed patent
                address = self._parse_address(dimension, record['_source']['owner'])
                if not address: continue #patent can't be located or it's not an indonesian patent
                lines.append(CreateCSVLine([id_application,id_certificate,status, date_begin, date_end, classes, address]))
        #print('done')
        #print(counter, 'from', str(len(obj['hits'])))
        return lines
    
    def _parse_address(self, dimension, owner_record, inventor_record=None):
        owner_address=None;inventor_address=None
        for owner in owner_record:
            if dimension==self.settings['DIMENSION_PATENT']:
                if owner['nationality']=='ID' or (owner['country'] and owner['country']['code']=='ID'):
                    #bisa dicek alamatnya di Indo juga?
                    owner_address=owner['alamat_pemegang'] if owner['alamat_pemegang'] != '-' else None 
                    if not owner_address: break
            elif dimension==self.settings['DIMENSION_TRADEMARK']:
                if owner['country_code']=='ID':
                    owner_address=owner['alamat_pemegang'] if owner['alamat_pemegang'] != '-' else None 
                    if not owner_address: break
        if dimension==self.settings['DIMENSION_PATENT'] and inventor_record:
            for inventor in inventor_record:
                if inventor['nationality']=='ID':
                    #bisa dicek alamatnya di Indo juga?
                    inventor_address=inventor['alamat_inventor'] if inventor['alamat_inventor'] != '-' else None
                    if not inventor_address: break
        return inventor_address if inventor_address else owner_address

    def _parse_classes(self, ipc_record):
        if not ipc_record: return None #handle unclassified patent
        record_list=[i['ipc_full'] for i in ipc_record]
        if len(record_list[0])>12: record_list=record_list[0].strip().split(',') #handle error value nempel
        ipc_list=[]
        for ipc in record_list:
            category = ipc.strip().split()
            if category[0] not in ipc_list: ipc_list.append(category[0])
        return ipc_list

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
        self._setup_minio_client(self.bucket)
        while True:
            self._aggregate()
            time.sleep(self.settings['SLEEP_TIME'])
