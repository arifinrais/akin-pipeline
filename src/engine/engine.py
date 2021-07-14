from os import stat
import sys

import redis
import config
import json
import traceback
from redis import Redis
from tenacity import retry
from jsonschema import validate
from abc import ABC, abstractmethod
import redis_lock
import time

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
        if job==self.settings['JOB_AGGREGATE']: return self.settings['LOCK_AGGREGATE']
        if job==self.settings['JOB_TRANSFORM']: return self.settings['LOCK_TRANSFORM']
        if job==self.settings['JOB_ANALYZE']: return self.settings['LOCK_ANALYZE']

    @classmethod
    def _get_after_job(self, job):
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
                    temp=self.redis_conn.get(_key)
                    if temp['job']==job and temp['status']==self.settings['STAT_WAIT']:
                        temp = self.redis_conn.get(_key)
                        temp['status'] = self.settings['STAT_WIP'] #update job status
                        self.redis_conn.set(_key, temp)
                        key, dimension, year = _key, temp['dimension'], temp['year']
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
                    temp = self.redis_conn.get(key)
                    temp['job'], temp['status'] = self._get_after_job(self, job), self.settings['STAT_WAIT']
                    self.redis_conn.set(key, temp)
                else:
                    temp = self.redis_conn.get(key)
                    temp['job'], temp['status'], temp['errormsg'] = job, self.settings['STAT_ERROR'], errormsg
                    self.redis_conn.set(key, temp)
                lock.release()
                break
            else:
                time.sleep(self.settings['SLEEP_TIME'])
    
    @staticmethod
    def wrong_input():
        None

    @staticmethod
    def error_handler():
        #logging(?)
        None

class Aggregator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_AGGREGATE']
    
    @classmethod
    def _aggregate(self):
        self._setup_redis_conn()
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._aggregate_records(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    @classmethod
    def _aggregate_records(self, dimension, year):
        #try
            #load the objects from minio
            #parse and aggregate, uniquify
            #save the aggregated file to minio
            #return True, None
        #except 
            #return False, errormsg
        None

    def start(self):
        while True:
            self._aggregate()
            time.sleep(self.settings['SLEEP_TIME'])

class Preparator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_TRANSFORM']
    
    @classmethod
    def _transform(self):
        self._setup_redis_conn()
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._transform_file(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    @classmethod
    def _transform_file(self, dimension, year):
        #try
            #load the file from minio
            #submit cleaning, pattern-matching(?), geocoding, encoding job to SPARK
            #save the result file to minio
            #return True, None
        #except
            #return False, errormsg
        None
    
    def start(self):
        while True:
            self._transform()
            time.sleep(self.settings['SLEEP_TIME'])

class Analytics(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_ANALYZE']
    
    @classmethod
    def _analyze(self):
        self._setup_redis_conn()
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._analyze_file(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    @classmethod
    def _analyze_file(self, dimension, year):
        #try
            #load the file from minio
            #analyze
            #save the analyses to mongodb
            #return True, None
        #except
            #return False, errormsg
        None
    
    def start(self):
        while True:
            self._transform()
            time.sleep(self.settings['SLEEP_TIME'])

def main():
    try:
        command = sys.argv[1]
        if command=='aggregate':
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

