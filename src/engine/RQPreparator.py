#!/usr/bin/env python3
import sys, time, logging, json, regex as re, requests as req
from minio.error import S3Error
from engine.Engine import Engine
from engine.EngineHelper import GenerateFileName, CreateCSVLine, BytesToLines, CleanAddress, PatternSplit, PostalSplit, Geocode
from io import BytesIO
from redis import Redis
from rq import Connection, Worker
from rq.queue import Queue
from rq.job import Job 

class RQPreparator(Engine):
    ADDR_COL_INDEX=6
    TEMP_FOLDERS={'mapped':'tmp_mapped','unmapped':'tmp_unmapped','result':'result','failed':'failed'}
    TFM_WORK={'clean':'cln','postal_mapping':'psp','pattern_matching':'ptm','geocode':'gcd'}
    TFM_WAIT_TIME=5

    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_TRANSFORM']
        self.bucket = self.settings['MINIO_BUCKET_TRANSFORMED']
        self.previous_bucket = self.settings['MINIO_BUCKET_AGGREGATED']
        self.resources_bucket = self.settings['MINIO_BUCKET_RESOURCES']
  
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
            success, errormsg = self._geocoding_in_rq(dimension, year)
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
            if not line_list: raise Exception('405: File Not Fetched')
            data_output = self._fetch_file_from_minio(self.resources_bucket, self.standard_file_region)
            std_file = json.load(BytesIO(data_output))
            #print(std_file[0])
            #cleaning the data
            ll_cleaned = self._rq_cleaning(line_list, self.ADDR_COL_INDEX)
            #print(ll_cleaned[0], len(ll_cleaned))
            #splitting the data based on postal code
            ll_mapped_postal, ll_unmapped = self._rq_split(ll_cleaned, std_file, self.TFM_WORK['postal_mapping'], self.ADDR_COL_INDEX)
            if not ll_unmapped:
                self._save_data_to_minio(ll_mapped_postal, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['result'])
                return True, True
            #print(ll_mapped_postal[0], len(ll_mapped_postal))
            #splitting the data based on pattern matching
            ll_mapped_pattern, ll_unmapped = self._rq_split(ll_unmapped, std_file, self.TFM_WORK['pattern_matching'],self.ADDR_COL_INDEX)
            #print(ll_mapped_pattern[0], len(ll_mapped_pattern))
            mapped_lines=[]
            if ll_mapped_postal:
                for line in ll_mapped_postal:
                    mapped_lines.append(CreateCSVLine(line))
            if ll_mapped_pattern:
                for line in ll_mapped_pattern:
                    mapped_lines.append(CreateCSVLine(line))
            if not ll_unmapped:
                self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['result'])
                return True, True
            
            #saving the data separately if there are still unmapped records (note: it can be that there's no mapped record)
            if mapped_lines:
                #print(mapped_lines)
                self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['mapped'])
            unmapped_lines = []
            for line in ll_unmapped:
                unmapped_lines.append(CreateCSVLine(line))
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

    def _rq_split(self, line_list, std_file, tfm_work, col_idx=6, api_config=None):
        job_id = []
        for line in line_list:
            with Connection():
                if tfm_work==self.TFM_WORK['postal_mapping']:
                    job = self.queue[tfm_work].enqueue(PostalSplit, args=(line, std_file, col_idx))
                elif tfm_work==self.TFM_WORK['pattern_matching']:
                    job = self.queue[tfm_work].enqueue(PatternSplit, args=(line, std_file, col_idx))
                elif tfm_work==self.TFM_WORK['geocode']:
                    job = self.queue[tfm_work].enqueue(Geocode, args=(line, std_file, api_config, col_idx))
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

    def _geocoding_in_rq(self, dimension, year):
        #set api config
        API_CONFIG={}
        mapped_fname=GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.TEMP_FOLDERS['mapped'])
        unmapped_fname=GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.TEMP_FOLDERS['unmapped'])
        try:
            data_output = self._fetch_file_from_minio(self.bucket, unmapped_fname)
            ll_unmapped = BytesToLines(data_output, line_list=True) if data_output else None
            if not ll_unmapped: raise Exception('405: File Not Fetched')
            data_output = self._fetch_file_from_minio(self.resources_bucket, self.standard_file_region)
            std_file = json.load(BytesIO(data_output))

            #geocoding and mapping data in spark
            ll_geomapped, ll_unmapped = self._rq_split(ll_unmapped, std_file, self.TFM_WORK['geocode'], self.ADDR_COL_INDEX, API_CONFIG)

            #open previously mapped dataset
            data_output = self._fetch_file_from_minio(self.bucket, mapped_fname)
            ll_mapped = BytesToLines(data_output, self.column_names) if data_output else None

            #saving joined mapped dataset and failed to transform dataset
            mapped_lines=[]
            if ll_mapped:
                for line in ll_mapped:
                    mapped_lines.append(CreateCSVLine(line))
            if ll_geomapped:
                for line in ll_geomapped:
                    mapped_lines.append(CreateCSVLine(line))
            if mapped_lines:
                self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['result'])
            if ll_unmapped:
                unmapped_lines=[]
                for line in ll_unmapped:
                    unmapped_lines.append(CreateCSVLine(line))
                self._save_data_to_minio(unmapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['failed'])
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    def start(self):
        setup_rq = self._setup_rq()
        setup_redis = self._setup_redis_conn()
        setup_minio = self._setup_minio_client(self.bucket)
        setup_resfile = self._load_resources_to_minio()
        setup = setup_rq and setup_redis and setup_minio and setup_resfile
        logging.info("Preparator Engine Successfully Started") if setup else logging.warning("Problem in Starting Preparator Engine")    
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
        