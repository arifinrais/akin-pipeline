#!/usr/bin/env python3
import sys, time, json, csv, traceback #, os, logging
from engine import config
from datetime import datetime
from rejson import Client, Path
from minio import Minio
from io import BytesIO, StringIO

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
        self.redis_conn = Client(host=self.settings['JOB_REDIS_HOST'], 
                            port=self.settings['JOB_REDIS_PORT'], 
                            password=self.settings['JOB_REDIS_PASSWORD'],
                            db=self.settings['JOB_REDIS_DB'],
                            decode_responses=True,
                            socket_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'],
                            socket_connect_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'])

    def _setup_minio_client(self, bucket_name=None):
        print('setup minio connection')
        '''
        self.minio_client = Minio(
            self.settings['MINIO_HOST']+':'+str(self.settings['MINIO_PORT']),
            access_key=self.settings['MINIO_ROOT_USER'],
            secret_key=self.settings['MINIO_ROOT_PASSWORD'],
            secure=False #koentji harus di set ntar di kubernetes kalau mau secure pake TLS
        )
        '''
        self.minio_client = Minio(
            'localhost:9000',
            access_key='minio',
            secret_key='minio123',
            secure=False, #koentji harus di set ntar di kubernetes kalau mau secure pake TLS
        )
        print('try to create bucket')
        if bucket_name:
            try: 
                if not self.minio_client.bucket_exists(bucket_name):
                    print('bucket not exist')
                    self.minio_client.make_bucket(bucket_name)
                print('not error')
            except:
                print(sys.exc_info())
   
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

    #@retry
    def _redis_update_stat_before(self, job):
        key, dimension, year = '', '', 0
        while True:
            updated = False
            try:
                with self.redis_conn.lock(self._get_lock_name(job), blocking_timeout=5) as lock:
                    for _key in self.redis_conn.scan_iter():
                        if _key==self._get_lock_name(job): continue #koentji
                        _job=json.loads(self.redis_conn.jsonget(_key, Path('.')))
                        if _job['job']==job and _job['status']==self.settings['STAT_WAIT']:
                            _job['status'] = self.settings['STAT_WIP']
                            self.redis_conn.jsonset(_key, Path.rootPath(), json.dumps(_job))
                            key, dimension, year = _key, _job['dimension'], _job['year']
                            updated=True
                            break
                if updated:
                    break
            except:
                time.sleep(2)#self.settings['SLEEP_TIME'])
        return key, dimension, year
        
    #@retry
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

    def _generate_file_name(self, bucket_base, dimension, year, extension, file_id=None):    
        if file_id:
            zero_prefix= '00' if file_id<10 else '0' if file_id <100 else ''
            _file_id = zero_prefix+str(file_id)
            return dimension+'/'+str(year)+'/'+bucket_base+'_'+dimension+'_'+str(year)+'_'+_file_id+extension       
        else:
            return dimension+'/'+bucket_base+'_'+dimension+'_'+str(year)+extension
      
    def _convert_lines_to_csv(self, lines):
        csv_file = StringIO()
        wr=csv.writer(csv_file, quoting=csv.QUOTE_NONE)
        for line in lines:
            wr.writerow(line)
        return csv_file
 
    def _create_csv_line(self, fields, delimiter="\t"):
        line = ""
        for i in range(len(fields)):
            if i<len(fields)-1:
                line=line+fields[i]+delimiter
            else:
                line=line+fields[i]
        return line
    
    def _parse_csv_line(self, line, delimiter="\t"):
        return line.strip().split(delimiter)
    
    def _save_lines_to_minio_in_csv(self, lines, bucket_identifier, dimension, year):
        csv_file=self._convert_lines_to_csv(lines)
        bucket_name=bucket_identifier
        file_name=self._generate_file_name(bucket_identifier, dimension, year,'.csv')
        content = csv_file.read().encode('utf-8')
        self.minio_client.put_object(bucket_name, file_name, BytesIO(content), length=-1, part_size=56*1024, content_type='application/csv') #assuming maximum csv filesize 50kb

#Handler and Logger        
def wrong_input(err):
    exc_type, exc_value, exc_traceback = err
    trace_back = traceback.extract_tb(exc_traceback)
    print(" >Exception Type: %s" % exc_type.__name__)
    print(" >Exception Message: %s" % exc_value)
    print(" >Stack Trace:")
    for trace in trace_back:
        print("  >file: %s, line: %d, funcName: %s, message: %s" % (trace[0], trace[1], trace[2], trace[3]))

def error_handler():
    #logging utils(?)
    None