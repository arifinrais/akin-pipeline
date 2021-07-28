import sys, time, json, csv, traceback #, os, logging
import localsettings as config
import requests as req
#from os import stat
#from tenacity import retry
#from jsonschema import validate
#from abc import ABC, abstractmethod
from datetime import datetime
from redis import Redis
from rejson import Client, Path
from rq.queue import Queue
from rq.job import Job 
from minio import Minio
from minio.error import S3Error
from io import BytesIO, StringIO
from copy import deepcopy
from pyspark.conf import SparkConf
from pyspark import sql
from pymongo import MongoClient

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

    def _setup_minio_client(self):
        self.minio_client = Minio(
            self.settings['MINIO_HOST']+':'+str(self.settings['MINIO_PORT']),
            access_key=self.settings['MINIO_ACCESS_KEY'],
            secret_key=self.settings['MINIO_SECRET_KEY'],
        )
   
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
                time.sleep(1)#self.settings['SLEEP_TIME'])
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
            return dimension=='ptn' or dimension=='trd'
        elif source=='SINTA':
            return dimension=='pub'    
    
    def _generate_file_id(self, file_id):
        if file_id<10:
            return '00'+str(file_id)
        elif file_id<100:
            return '0'+str(file_id)
        else:
            return str(file_id)
 
    def _generate_file_name(self, bucket_base, dimension, year, extension, file_id=None):    
        if file_id:
            self._generate_file_id(file_id)
            return dimension+'/'+str(year)+'/'+bucket_base+'_'+dimension+'_'+str(year)+'_'+file_id+extension       
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
            
    @staticmethod
    def wrong_input(err):
        exc_type, exc_value, exc_traceback = err
        trace_back = traceback.extract_tb(exc_traceback)
        print(" >Exception Type: %s" % exc_type.__name__)
        print(" >Exception Message: %s" % exc_value)
        print(" >Stack Trace:")
        for trace in trace_back:
            print("  >file: %s, line: %d, funcName: %s, message: %s" % (trace[0], trace[1], trace[2], trace[3]))
    
    @staticmethod
    def error_handler():
        #logging utils(?)
        None

class Ingestor(Engine):
    def __init__(self, wait_ingest_cycle=5):
        Engine.__init__(self)
        self.job = self.settings['JOB_INGEST']
        self.wait_ingest_cycle = wait_ingest_cycle

    def _ingest(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._ingest_records(dimension, year)
        self._redis_update_stat_after(key, self.job, success, errormsg)
  
    def _setup_rq(self):
        self.rq_conn = Redis(host=self.settings['RQ_REDIS_HOST'], 
                            port=self.settings['RQ_REDIS_PORT'], 
                            password=self.settings['RQ_REDIS_PASSWORD'],
                            db=self.settings['RQ_REDIS_DB'],
                            decode_responses=True,
                            socket_timeout=self.settings['RQ_REDIS_SOCKET_TIMEOUT'],
                            socket_connect_timeout=self.settings['RQ_REDIS_SOCKET_TIMEOUT'])
        self.ingest_queue = Queue(connection=self.rq_conn)
 
    def _ingest_records(self, dimension, year):
        try:
            req_list = self._generate_req_list(dimension, year)
            print(req_list[0]['url'])
            job_id = []
            file_id = 1
            for req_item in req_list:
                job = Job.create(self._fetch_and_save, (req_item, dimension, year, file_id)) #can set the id if you want
                job_id.append(job.id)
                self.ingest_queue.enqueue(job)
                file_id+=1
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
    
    #can be upgraded to async? @async/retry/classmethod
    def _fetch_and_save(self, arguments):
        req_item, dimension, year, file_id = arguments
        bucket_name=self.settings['MINIO_INGESTED_IDENTIFIER']
        if not self.minio_client.bucket_exist(bucket_name):
            self.minio_client.make_bucket(bucket_name)
        if self._check_dimension_source('PDKI', dimension):
            file_name=self._generate_file_name(self.settings['MINIO_INGESTED_IDENTIFIER'],dimension,year,'_json',file_id)
            resp=req.get(req_item['url'])
            resp_dict = resp.json()
            content = json.dumps(resp_dict['hits']['hits'], ensure_ascii=False).encode('utf-8') # convert dict to bytes
            self.minio_client.put_object(bucket_name, file_name, BytesIO(content), length=-1, part_size=1024*1024, content_type='application/json') #assuming maximum json filesize 1MB
        elif self._check_dimension_source('SINTA', dimension):
            file_name=self._generate_file_name(self.settings['MINIO_INGESTED_IDENTIFIER'],dimension,year,'_html',file_id)
            resp=req.get(req_item['url'])
            content=resp.text.encode('utf-8') #convert text/html to bytes for reverse conversion use bytes.decode()
            self.minio_client.put_object(bucket_name, file_name, BytesIO(content), length=-1, part_size=1024*1024, content_type='text/html') #assuming maximum html filesize 1MB

    def _generate_req_list(self, dimension, year):
        req_list=[]
        if self._check_dimension_source('PDKI', dimension):
            #source: PDKI
            base_url="https://pdki-indonesia-api.dgip.go.id/api/"
            param_type, param_keywords, param_dates = self._generate_parameters(dimension, year)
            for keyword in param_keywords:
                for date in param_dates:
                    req_item={}
                    req_item['url']=base_url+param_type\
                        +"/search?keyword="+keyword\
                        +"&start_tanggal_dimulai_perlindungan="+date[0]\
                        +"&end_tanggal_dimulai_perlindungan="+date[1]\
                        +"&type="+param_type\
                        +"&order_state=asc&page=1"
                    req_item['header']= self._generate_header(dimension)
                    req_list.append(req_item)
        return req_list

    def _generate_parameters(self, dimension, year):
        if self._check_dimension_source('PDKI', dimension):
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
        elif self._check_dimension_source('SINTA', dimension):
            #add parameters for SINTA here
            return None

    def _generate_header(self, dimension):
        if self._check_dimension_source('PDKI', dimension):
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
        elif self._check_dimension_source('PDKI', dimension):
            #add header for SINTA here
            None
        return header

    def start(self):
        self._setup_rq()
        self._setup_redis_conn()
        self._setup_minio_client()
        while True:
            self._ingest()
            time.sleep(self.settings['SLEEP_TIME'])

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
        None

if __name__ == "__main__":
    sys.exit(main())

