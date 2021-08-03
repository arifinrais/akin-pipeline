#!/usr/bin/env python3
import sys, time, logging, json, regex as re, csv, traceback
import requests as req
import pandas as pd
from engine.Engine import Engine
from engine.EngineHelper import GenerateFileName, BytesToDataFrame, CreateCSVLine, BytesToLines, CleanAddress, PatternSplit, PostalSplit
from io import BytesIO
from redis import Redis
from minio import Minio
from minio.error import S3Error
from fuzzywuzzy import fuzz
import logging
from engine.Engine import Engine
from engine.EngineHelper import Scrape
from redis import Redis
from rq import Connection, Worker
from rq.queue import Queue
from rq.job import Job 

class RQPreparator(Engine):
    ADDR_COL_INDEX=6
    TEMP_FOLDERS={'mapped':'tmp_mapped','unmapped':'tmp_unmapped','result':'result'}
    TFM_WORK={'clean':'cln','postal_mapping':'psp','pattern_matching':'ptm','geocode':'gcd'}
    TFM_WAIT_TIME=5

    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_TRANSFORM']
        self.bucket = self.settings['MINIO_BUCKET_TRANSFORMED']
        self.previous_bucket = self.settings['MINIO_BUCKET_AGGREGATED']
        self.resources_bucket = self.settings['MINIO_BUCKET_RESOURCES']
        self.standard_file_region = self.settings['FILE_REGION_STANDARD']
        self.standard_file_department = self.settings['FILE_DEPARTMENT_STANDARD']
  
    def _setup_rq(self):
        try:
            self.rq_conn = Redis(host=self.settings['RQ_REDIS_HOST'], 
                                port=self.settings['RQ_REDIS_PORT'], 
                                password=self.settings['RQ_REDIS_PASSWORD'],
                                db=self.settings['RQ_REDIS_DB'],
                                decode_responses=False, #koentji
                                socket_timeout=self.settings['RQ_REDIS_SOCKET_TIMEOUT'],
                                socket_connect_timeout=self.settings['RQ_REDIS_SOCKET_TIMEOUT'])
            self.queue={}
            self.queue[self.TFM_WORK['clean']] = Queue(self.TFM_WORK['clean'], connection=self.rq_conn)
            self.queue[self.TFM_WORK['postal_mapping']]= Queue(self.TFM_WORK['postal_mapping'], connection=self.rq_conn)
            self.queue[self.TFM_WORK['pattern_matching']] = Queue(self.TFM_WORK['pattern_matching'], connection=self.rq_conn)
            self.queue[self.TFM_WORK['geocode']] = Queue(self.TFM_WORK['geocode'], connection=self.rq_conn)
            return True
        except:
            return False
   
    def _transform(self):
        logging.debug('Acquiring Lock for Transformation Jobs...')
        key, dimension, year = self._redis_update_stat_before(self.job)
        logging.debug('Transforming Records...')
        success, errormsg = self._transform_in_rq(dimension, year)
        logging.debug('Do Geocoding...')
        if success and not errormsg:
            success, errormsg = self._geocoding(dimension, year)
        logging.debug('Updating Job Status...')
        self._redis_update_stat_after(key, self.job, success, errormsg)
        #success, errormsg = self._transform_in_rq('ptn', 2018) #for debugging
        #success, errormsg = self._geocoding('ptn', 2018) #for debugging
        #print(success, errormsg)

    def _transform_in_rq(self, dimension, year):
        file_name=GenerateFileName(self.previous_bucket, dimension, year, 'csv')
        try:
            data_output = self._fetch_file_from_minio(self.previous_bucket, file_name)
            line_list = BytesToLines(data_output, line_list=True) if data_output else None
            data_output = self._fetch_file_from_minio(self.resources_bucket, self.standard_file_region)
            std_file = json.load(BytesIO(data_output))
            
            ll_cleaned = self._rq_cleaning(line_list, self.ADDR_COL_INDEX)
            ll_mapped_postal, ll_unmapped = self._rq_postal_split(ll_cleaned, std_file, self.ADDR_COL_INDEX)
            ll_mapped_pattern, ll_unmapped = self._rq_pattern_split(ll_unmapped, std_file, self.ADDR_COL_INDEX)

            mapped_lines=[]
            for line in ll_mapped_postal:
                mapped_lines.append(CreateCSVLine(line))
            for line in ll_mapped_pattern:
                mapped_lines.append(CreateCSVLine(line))
            unmapped_lines = []
            for line in ll_unmapped:
                unmapped_lines.append(CreateCSVLine(line))
            
            if mapped_lines and not unmapped_lines:
                self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['result'])
                return True, True
            else:
                self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['mapped'])
                self._save_data_to_minio(unmapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['unmapped'])
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    def _rq_cleaning(self, line_list, col_idx=6):
        REGEXP_LIST = [
            #remove long spaces
            ("\s+", " "),
            #remove postal/correspondence address
            ("(?i)[\s\*\(][Aa]lamat\s[SsKk2].*$", ""),
            #remove previous address
            ("(?i)\(?perubahan\salamat.*$", ""),
            #remove telephone number
            (("(?i)\s(telp?\.|telp\s).*indonesia\s+?$", ", INDONESIA")),
            ("(?i)\s(telp?\.|telp\s).*$", ""),
            #remove to be noted remarks
            ("(?i)\(\s?u\.?p\.?.*\)", ""),
            ("(?i)\s\(?\s?u\.?p\.?\s[a-z].*indonesia\s+?$", ", INDONESIA"),
            ("(?i)\s\(?\s?u\.?p\.?\s[a-z].*$", ""),
            #remove general remarks non-postal code
            ("\((?!\d{5}).*\)", ""),
            #fix commas position
            (",\s,(\s,)*", ", "),
            (",+", ","),
            #remove other remaining clutter
            ("¿+", ""),
            ("#+", ""),
            ("·+", ""),
            (";", ""),
            #remove long spaces again
            ("\s+", " ")]
        COUNTRY_LIST = [
            'india','gujarat',
            'u.s.a','california','indiana','san diego, ca','united states', 
            'korea', 
            'china', 
            'thailand', 
            'singapore', 
            'switzerland', 
            'japan', 'tokyo',
            'germany', 'erlangen',
            'sweden', 
            'netherlands', 
            'italy', 
            'belgium', 
            'philippines', 
            'malaysia', 
            'france', 
            'norway', 
            'united kingdom', 
            'finland'] 
        job_id = []
        for line in line_list:
            with Connection():
                job = self.queue[self.TFM_WORK['clean']].enqueue(CleanAddress, args=(line, REGEXP_LIST, COUNTRY_LIST, col_idx))
                job_id.append(job.id)
        ll_cleaned=[]
        while True:
            if len(job_id):
                for id in job_id:
                    job = Job(id, self.rq_conn)
                    #print(job.result) #just checkin'
                    if job.get_status()=='finished':
                        if job.result: ll_cleaned.append(job.result)
                        job_id.remove(id)
            else:
                break
            time.sleep(self.TFM_WAIT_TIME)     
        return ll_cleaned

    def _rq_split(self, line_list, std_file, tfm_work, col_idx=6):
        job_id = []
        for line in line_list:
            with Connection():
                if tfm_work==self.TFM_WORK['postal_mapping']:
                    job = self.queue[tfm_work].enqueue(PostalSplit, args=(line, std_file, col_idx))
                elif tfm_work==self.TFM_WORK['pattern_matching']:
                    job = self.queue[tfm_work].enqueue(PatternSplit, args=(line, std_file, col_idx))
                job_id.append(job.id)
        ll_mapped=[];ll_unmapped=[]
        while True:
            if len(job_id):
                for id in job_id:
                    job = Job(id, self.rq_conn)
                    #print(job.result) #just checkin'
                    if job.get_status()=='finished':
                        if job.result: 
                            line_mapped, line_unmapped = job.result
                            ll_mapped.append(line_mapped) if line_mapped else ll_unmapped.append(line_unmapped)
                        job_id.remove(id)
            else:
                break
            time.sleep(self.TFM_WAIT_TIME)     
        return ll_mapped, ll_unmapped

    def _geocoding(self, dimension, year):
        mapped_fname=GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.TEMP_FOLDERS['mapped'])
        try:
            #open unmapped dataset
            unmapped_fname=GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.TEMP_FOLDERS['unmapped'])
            data_output = self._fetch_file_from_minio(self.bucket, unmapped_fname)
            df = BytesToDataFrame(data_output, self.column_names) if data_output else None
            data_output = self._fetch_file_from_minio(self.resources_bucket, self.standard_file_region)
            std_file = json.load(BytesIO(data_output))
            
            #geocode unmapped data in spark
            #bisa _geocoding_in_rq atau _geocoding_bare
            df_geocoded = self._spark_geocoding(df, self.column_names[-1])
            _df_mapped = self._spark_mapping_gc(df_geocoded, std_file, self.column_names[-1])

            #open mapped dataset
            mapped_fname = GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.TEMP_FOLDERS['mapped'])
            data_output = self._fetch_file_from_minio(self.bucket, mapped_fname)
            df = BytesToDataFrame(data_output, self.column_names) if data_output else None

            #join and save mapped and geocoded data
            df_mapped = df.append(_df_mapped)
            mapped_lines = []
            for mapped_values in df_mapped.values.tolist():
                mapped_lines.append(CreateCSVLine(mapped_values))
            self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['result']) #buat bucket khusus?
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
        pass
        
    def _spark_geocoding(self, dataframe, col_name="_c6"):
        #setup spark udfs
        def geocode(s,_config):
            #config request sesuai APInya
            #hit api
            #parse response
            #return parsed response
            pass
        
        def udf_geocode(_config):
            return udf(lambda x: geocode(x,_config), StringType())
        
        #setup config
        _config={} #sesuain sama APInya

        #setup spark
        spark_conf = self._setup_spark(app_name='data_splitting_postal')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        
        #geocoding based on bare address
        df = spark_session.createDataFrame(dataframe)
        df_geocoded = df.withColumn(col_name, udf_geocode(_config)(col(col_name))).toPandas()
        
        spark_session.stop()
        return df_geocoded

    def _spark_mapping_gc(self, dataframe, std_file, col_name="_c6"):
        #setup spark udfs
        def map_geocode(s,std_file):
            fuzz_calc=lambda x,y: (fuzz.ratio(x,y)+fuzz.partial_ratio(x,y)+fuzz.token_sort_ratio(x,y)+fuzz.token_set_ratio(x,y))/4
            maxRtg, maxLoc = 0, None
            locs=s.strip().split('\t') #sesuain format hasil parsing geocode, misal <city>\t<province>
            fuzz_calc=lambda x,y: (fuzz.ratio(x,y)+fuzz.partial_ratio(x,y)+fuzz.token_sort_ratio(x,y)+fuzz.token_set_ratio(x,y))/4
            for rec in std_file:
                _prov = rec['province'].lower()
                _city = rec['city'].lower()
                rating = (fuzz_calc(locs[0],_city)+fuzz_calc(locs[1],_prov))/2
                if rating>maxRtg:
                    maxLoc=rec['city']+'\t'+rec['province']
            return maxLoc if maxLoc else 'GEOCODE_MAPPING_ERROR'
        
        def udf_map_geocode(std_file):
            return udf(lambda x: map_geocode(x,std_file), StringType())

        #setup spark
        spark_conf = self._setup_spark(app_name='data_splitting_postal')
        spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark_context = spark_session.sparkContext
        spark_context.setLogLevel("ERROR")
        
        #mapping geocoded address
        df = spark_session.createDataFrame(dataframe)
        df_mapped = df.withColumn(col_name, udf_map_geocode(std_file)(col(col_name))).toPandas()
        
        spark_session.stop()
        return df_mapped

    def _geocoding_in_rq(self, dimension, year):
        #set request list
        #loop to enqueue
        #wait for job
        #return result
        pass
        
    def _geocoding_bare(self, dimension, year):
        pass

    def start(self):
        setup_rq = self._setup_rq()
        setup_redis = self._setup_redis_conn()
        setup_minio = self._setup_minio_client(self.bucket)
        logging.info("Preparator Engine Successfully Started") if  setup_rq and setup_redis and setup_minio else logging.warning("Problem in Starting Preparator Engine")    
        #self._transform() #for debugging
        #return
        while True:
            self._transform()
            time.sleep(self.settings['SLEEP_TIME'])
    
    def prepare(self):
        self._setup_rq()
        with Connection():
            queues=[self.queue[self.TFM_WORK['clean']],self.queue[self.TFM_WORK['postal_mapping']],self.queue[self.TFM_WORK['pattern_matching']],self.queue[self.TFM_WORK['geocoding']]]
            worker = Worker(queues=queues, connection=self.rq_conn)
            worker.work()
            while True:
                if not worker.work():
                    worker.work()
        