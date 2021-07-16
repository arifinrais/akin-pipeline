from os import stat
import sys

import redis
from rq.queue import Queue
import config
import json
import traceback
from redis import Redis
from tenacity import retry
from jsonschema import validate
#from abc import ABC, abstractmethod
import redis_lock
import time
# for ingestion
from rq import Queue 
from rq.job import Job 
import requests as req
from minio import Minio
from minio.error import S3Error
from io import BytesIO

class Engine(object):
    def __init__(self):
        try:
            configs=[item for item in dir(config) if not item.startswith("__")]
            self.settings={}
            for item in configs:
                self.settings[item]=getattr(config, item)
            self.job_schema={
                "type": "object",
                "properties": {
                    "dimension": {"type": "string", "pattern": "^ptn|pub|trd$"},
                    "year": {"type": "integer", "minimum": self.settings['MIN_SCRAPE_YEAR'], "maximum": self.settings['MAX_SCRAPE_YEAR']},
                    "job": {"type": "string", "pattern": "^agg|tfm|anl$"},
                    "status": {"type": "string", "pattern": "^wait|wip|done|err$"},
                    "timestamp": {"type": "date-time"},
                    "errormsg": {"type": "string"}
                }
            }
        except:
            self.error_handler(sys.exc_info())
    
    def _setup_redis_conn(self):
        self.redis_conn = Redis(host=self.settings['JOB_REDIS_HOST'], 
                            port=self.settings['JOB_REDIS_PORT'], 
                            password=self.settings['JOB_REDIS_PASSWORD'],
                            db=self.settings['JOB_REDIS_DB'],
                            decode_responses=True,
                            socket_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'],
                            socket_connect_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'])

    @classmethod
    def _get_lock_name(self, job):
        if job==self.settings['JOB_INGEST']: return self.settings['LOCK_INGEST']
        if job==self.settings['JOB_AGGREGATE']: return self.settings['LOCK_AGGREGATE']
        if job==self.settings['JOB_TRANSFORM']: return self.settings['LOCK_TRANSFORM']
        if job==self.settings['JOB_ANALYZE']: return self.settings['LOCK_ANALYZE']

    @classmethod
    def _get_after_job(self, job):
        if job==self.settings['JOB_INGEST']: return self.settings['JOB_AGGREGATE']
        if job==self.settings['JOB_AGGREGATE']: return self.settings['JOB_TRANSFORM']
        if job==self.settings['JOB_TRANSFORM']: return self.settings['JOB_ANALYZE']
        if job==self.settings['JOB_ANALYZE']: return self.settings['JOB_ANALYZE']

    #@retry
    def _redis_update_stat_before(self, job):
        lock = redis_lock.Lock(self.redis_conn, self._get_lock_name(job))
        key, dimension, year = '', '', 0
        while True:
            if lock.acquire(blocking=False):
                for _key in self.redis_conn.scan_iter():
                    _job=self.redis_conn.get(_key)
                    if _job['job']==job and _job['status']==self.settings['STAT_WAIT']:
                        _job = self.redis_conn.get(_key)
                        _job['status'] = self.settings['STAT_WIP'] #update job status
                        self.redis_conn.set(_key, _job)
                        key, dimension, year = _key, _job['dimension'], _job['year']
                        break
                lock.release()
                break
            else:
                time.sleep(self.settings['SLEEP_TIME'])
        return key, dimension, year
        
    #@retry
    def _redis_update_stat_after(self, key, job, success, errormsg):
        lock = redis_lock.Lock(self.redis_conn, "aggregate_lock")
        while True:
            if lock.acquire(blocking=False):
                if success:
                    _job = self.redis_conn.get(key)
                    _job['job'], _job['status'] = self._get_after_job(self, job), self.settings['STAT_WAIT']
                    self.redis_conn.set(key, _job)
                else:
                    _job = self.redis_conn.get(key)
                    _job['job'], _job['status'], _job['errormsg'] = job, self.settings['STAT_ERROR'], errormsg
                    self.redis_conn.set(key, _job)
                lock.release()
                break
            else:
                time.sleep(self.settings['SLEEP_TIME'])
    
    @staticmethod
    def wrong_input():
        None

    @staticmethod
    def error_handler():
        #logging utils(?)
        None

class Ingestor(Engine):
    def __init__(self, wait_ingest_cycle=5):
        Engine.__init__(self)
        self.job = self.settings['JOB_INGEST']
        self.wait_ingest_cycle = wait_ingest_cycle
    
    @classmethod
    def _ingest(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._ingest_records(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)

    @classmethod
    def _setup_rq(self):
        self.rq_conn = Redis(host=self.settings['RQ_REDIS_HOST'], 
                            port=self.settings['RQ_REDIS_PORT'], 
                            password=self.settings['RQ_REDIS_PASSWORD'],
                            db=self.settings['RQ_REDIS_DB'],
                            decode_responses=True,
                            socket_timeout=self.settings['RQ_REDIS_SOCKET_TIMEOUT'],
                            socket_connect_timeout=self.settings['RQ_REDIS_SOCKET_TIMEOUT'])
        self.ingest_queue = Queue(self.rq_conn)
    
    @classmethod
    def _setup_minio(self):
        self.minio_client = Minio(
            self.settings['MINIO_HOST']+':'+self.settings['MINIO_PORT'],
            access_key=self.settings['MINIO_ACCESS_KEY'],
            secret_key=self.settings['MINIO_SECRET_KEY'],
        )

    @classmethod
    def _ingest_records(self, dimension, year):
        try:
            req_list = EngineHelper.generate_req_list(dimension, year)
            job_id = []
            _id = 1
            for req_item in req_list:
                file_id = EngineHelper.generate_file_id(_id)
                job = Job.create(self._fetch_and_save, (req_item, dimension, year, file_id)) #can set the id if you want
                job_id.append(job.id)
                self.ingest_queue.enqueue(job)
                _id+=1
            while True:
                job_done = True
                for id in job_id:
                    job = Job.fetch(id, self.rq_conn)
                    if job.get_status()!='finished':
                        job_done=False
                        break
                if job_done:
                    break
                time.sleep(self.wait_ingest_cycle)
            return True, None
        except:
            # in the meantime error message is just its value
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    @classmethod #can be upgraded to async?
    def _fetch_and_save(self, args):
        req_item, dimension, year, file_id = args
        bucket_name="raw/"+dimension+str(year)
        if not self.minio_client.bucket_exist(bucket_name):
            self.minio_client.make_bucket(bucket_name)
        if EngineHelper.check_dimension_source('PDKI', dimension):
            file_name=EngineHelper.generate_file_name('raw',dimension,year,file_id,'_json')
            resp=req.get(req_item['url'])
            resp_dict = resp.json()
            content = json.dumps(resp_dict['hits']['hits'], ensure_ascii=False).encode('utf-8') # convert dict to bytes
            self.minio_client.put_object(bucket_name, file_name, BytesIO(content), length=-1, part_size=1024*1024, content_type='application/json') #assuming maximum json filesize 1MB
        elif EngineHelper.check_dimension_source('SINTA', dimension):
            file_name=EngineHelper.generate_file_name('raw',dimension,year,file_id,'_html')
            resp=req.get(req_item['url'])
            content=resp.text.encode('utf-8') #convert text/html to bytes for reverse conversion use bytes.decode()
            self.minio_client.put_object(bucket_name, file_name, BytesIO(content), length=-1, part_size=1024*1024, content_type='text/html') #assuming maximum html filesize 1MB

    def start(self):
        self._setup_rq()
        self._setup_redis_conn()
        while True:
            self._ingest()
            time.sleep(self.settings['SLEEP_TIME'])

class Aggregator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_AGGREGATE']
    
    @classmethod
    def _aggregate(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._aggregate_records(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    @classmethod
    def _aggregate_records(self, dimension, year):
        try:
            #load the objects from minio
            #parse and aggregate, uniquify
            #save the aggregated file to minio
            #return True, None
            None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    def start(self):
        self._setup_redis_conn()
        while True:
            self._aggregate()
            time.sleep(self.settings['SLEEP_TIME'])

class Preparator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_TRANSFORM']
    
    @classmethod
    def _transform(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._transform_file(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    @classmethod
    def _transform_file(self, dimension, year):
        try:
            #load the file from minio
            #submit cleaning, pattern-matching(?), geocoding, encoding job to SPARK
            #save the result file to minio
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    def start(self):
        self._setup_redis_conn()
        while True:
            self._transform()
            time.sleep(self.settings['SLEEP_TIME'])

class Analytics(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_ANALYZE']
    
    @classmethod
    def _analyze(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._analyze_file(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    @classmethod
    def _analyze_file(self, dimension, year):
        try:
            #load the file from minio
            #analyze
            #save the analyses to mongodb
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    def start(self):
        self._setup_redis_conn()
        while True:
            self._transform()
            time.sleep(self.settings['SLEEP_TIME'])

class EngineHelper():
    @staticmethod
    def generate_req_list(dimension, year):
        req_list=[]
        req_item={}
        if EngineHelper.check_dimension_source('PDKI', dimension):
            #source: PDKI
            base_url="https://pdki-indonesia-api.dgip.go.id/api/"
            param_type, param_keywords, param_dates = EngineHelper.generate_parameters(dimension, year)
            for keyword in param_keywords:
                for date in param_dates:
                    req_item['url']=base_url+param_type\
                        +"/search?keyword="+keyword\
                        +"&start_tanggal_dimulai_perlindungan="+date[0]\
                        +"&end_tanggal_dimulai_perlindungan="+date[1]\
                        +"&type="+param_type\
                        +"&order_state=asc&page=1"
                    req_item['header']= EngineHelper.generate_header(dimension)
                    #req_item['body']= if needed
        elif EngineHelper.check_dimension_source('SINTA', dimension):
            #source: SINTA
            #SHOULD BE DEPARTMENTAL APPROACH, DEPARTMENT IS ALREADY A SUBJECT
            #https://sinta.ristekbrin.go.id/departments/detail?page=1&afil=379&id=46001&view=documentsscopus
            #loop for every afil
                #loop for every id
                    #filter based on year?
            None
        return req_list

    @staticmethod
    def generate_parameters(dimension, year):
        if EngineHelper.check_dimension_source('PDKI', dimension):
            param_type=''
            param_keywords=[]
            param_dates=[]
            _year=str(year)
            if dimension=='ptn':
                param_type='patent'
                param_keywords=['DID','D00','J00','K00','M00','R00','V00']
            elif dimension=='trd':
                param_type='trademark'
                param_keywords=['PID','P00','S00','W00']
            for i in range(len(param_keywords)):
                param_keywords[i]=param_keywords[i]+_year
            month_28=[2]
            month_30=[1,3,5,7,8,10,12]
            month_31=[4,6,9,11]
            for i in range(12):
                _month=str(i+1)
                if i+1<10: _month='0'+_month
                date_base=_year+'-'+_month+'-'
                if i+1 in month_28:
                    param_dates.append([date_base+'01',date_base+'28'])
                if i+1 in month_30:
                    param_dates.append([date_base+'01',date_base+'30'])
                if i+1 in month_31:
                    param_dates.append([date_base+'01',date_base+'31'])
            return param_type, param_keywords, param_dates
        elif EngineHelper.check_dimension_source('SINTA', dimension):
            #add parameters for SINTA here
            return None

    @staticmethod
    def generate_header(dimension):
        if EngineHelper.check_dimension_source('PDKI', dimension):
            # if pairKey needed it can be implemented here
            header = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Host': 'pdki-indonesia-api.dgip.go.id',
                'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
                'sec-ch-ua-mobile': '?0',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
            }
        elif EngineHelper.check_dimension_source('PDKI', dimension):
            #add header for SINTA here
            None
        return header

    @staticmethod
    def check_dimension_source(source, dimension):
        if source=='PDKI':
            return dimension=='ptn' or dimension=='trd'
        elif source=='SINTA':
            return dimension=='pub'    
    
    @staticmethod
    def generate_file_id(file_id):
        if file_id<10:
            return '00'+str(file_id)
        elif file_id<100:
            return '0'+str(file_id)
        else:
            return str(file_id)

    @staticmethod
    def generate_file_name(bucket_base, dimension, year, file_id, extension):
        return bucket_base+'_'+dimension+'_'+str(year)+'_'+file_id+extension

def main():
    try:
        command = sys.argv[1]
        if command=='ingest':
            engine = Ingestor()
            engine.start()
        elif command=='aggregate':
            engine = Aggregator()
            engine.start()
        elif command=='transform':
            engine = Preparator()
            engine.start()
        elif command=='analyze':
            engine = Analytics()
            engine.start()
        else:
            raise ValueError
    except KeyboardInterrupt:
        print("Turning Off The Engine...")
    except:
        Engine.wrong_input(sys.exc_info())

if __name__ == "__main__":
    sys.exit(main())

