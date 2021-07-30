#!/usr/bin/env python3
import sys, time, json
from typing import Type
from engine import config
from engine.EngineHelper import GenerateFileName
from datetime import datetime
from rejson import Client, Path
from minio import Minio
from io import BytesIO,StringIO

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
                    "year": {"type": "integer", "minimum": self.settings['MIN_INGEST_YEAR'], "maximum": self.settings['MAX_INGEST_YEAR']},
                    "job": {"type": "string", "pattern": "^agg|tfm|anl$"},
                    "status": {"type": "string", "pattern": "^wait|wip|done|err$"},
                    "timestamp": {"type": "date-time"},
                    "errormsg": {"type": "string"}
                }
            }
        except:
            self.error_handler(sys.exc_info())
    
    def _setup_redis_conn(self):
        try:
            self.redis_conn = Client(host=self.settings['JOB_REDIS_HOST'], 
                                port=self.settings['JOB_REDIS_PORT'], 
                                password=self.settings['JOB_REDIS_PASSWORD'],
                                db=self.settings['JOB_REDIS_DB'],
                                decode_responses=True,
                                socket_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'],
                                socket_connect_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'])
            return True
        except:
            return False

    def _setup_minio_client(self, bucket_name=None):
        self.minio_client = Minio(
            self.settings['MINIO_HOST']+':'+str(self.settings['MINIO_PORT']),
            access_key=self.settings['MINIO_ROOT_USER'],
            secret_key=self.settings['MINIO_ROOT_PASSWORD'],
            secure=False #koentji harus di set ntar di kubernetes kalau mau secure pake TLS
        )
        if bucket_name:
            try: 
                if not self.minio_client.bucket_exists(bucket_name):
                    self.minio_client.make_bucket(bucket_name)
            except:
                return False
        return True
   
    def _get_lock_name(self, job):
        if job==self.settings['JOB_INGEST']: return self.settings['LOCK_INGEST']
        if job==self.settings['JOB_AGGREGATE']: return self.settings['LOCK_AGGREGATE']
        if job==self.settings['JOB_TRANSFORM']: return self.settings['LOCK_TRANSFORM']
        if job==self.settings['JOB_ANALYZE']: return self.settings['LOCK_ANALYZE']
    
    def _get_after_job(self, job):
        if job==self.settings['JOB_INGEST']: return self.settings['JOB_AGGREGATE']
        if job==self.settings['JOB_AGGREGATE']: return self.settings['JOB_TRANSFORM']
        if job==self.settings['JOB_TRANSFORM']: return self.settings['JOB_ANALYZE']
        if job==self.settings['JOB_ANALYZE']: return self.settings['JOB_ANALYZE']

    def _redis_update_stat_before(self, job):
        key, dimension, year = '', '', 0
        while True:
            updated = False
            try:
                with self.redis_conn.lock(self._get_lock_name(job), blocking_timeout=5) as lock:
                    for _key in self.redis_conn.scan_iter(match='[pt][rtu][bdn]_[0-9][0-9][0-9][0-9]',count=100): #match="[pt][rtu][bdn]_[0-9][0-9][0-9][0-9]"
                        if _key==self._get_lock_name(job): continue
                        try: #ini bingung kadang kalau gapake json.loads gakebaca, tapi kalau gaada kadang TypeError
                            _job=json.loads(self.redis_conn.jsonget(_key, Path('.')))
                        except TypeError:
                            _job=self.redis_conn.jsonget(_key, Path('.'))
                        if _job and _job['job']==job and _job['status']==self.settings['STAT_WAIT']: #if _job penting bet
                            _job['status'] = self.settings['STAT_WIP']
                            self.redis_conn.jsonset(_key, Path.rootPath(), json.dumps(_job))
                            key, dimension, year = _key, _job['dimension'], _job['year']
                            updated=True
                            break
            finally:
                if updated: break
                time.sleep(2)#self.settings['SLEEP_TIME'])
        return key, dimension, year
        
    def _redis_update_stat_after(self, key, job, success, errormsg):
        while True:
            try:
                with self.redis_conn.lock(self._get_lock_name(job), blocking_timeout=5) as lock:    
                    _job=json.loads(self.redis_conn.jsonget(key, Path('.')))
                    _job['timestamp'] = datetime.utcnow().isoformat()
                    if success:
                        _job['job'], _job['status'] = self._get_after_job(job), self.settings['STAT_WAIT']
                    else:
                        _job['status'], _job['errormsg'] = self.settings['STAT_ERROR'], errormsg
                    self.redis_conn.jsonset(key, Path.rootPath(), json.dumps(_job))
                    break
            except:
                time.sleep(1)#self.settings['SLEEP_TIME'])
    
    def _check_dimension_source(self, source, dimension):
        if source=='PDKI': 
            return dimension==self.settings['DIMENSION_PATENT'] or dimension==self.settings['DIMENSION_TRADEMARK']
        elif source=='SINTA': 
            return dimension==self.settings['DIMENSION_PUBLICATION']
        else: 
            return False

    def _save_lines_to_minio_in_csv(self, lines, bucket_name, dimension, year):
        file_name=GenerateFileName(bucket_name, dimension, year,'.csv')
        csv_file = StringIO(newline='\n')
        for line in lines: csv_file.writelines(line)
        content = BytesIO(csv_file.getvalue().encode('utf-8'))
        self.minio_client.put_object(bucket_name, file_name, content, length=-1, part_size=5*1024*1024, content_type='application/csv') #assuming maximum csv filesize 50kb
