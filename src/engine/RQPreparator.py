#!/usr/bin/env python3
import sys, time, logging, json, regex as re, requests as req, pandas as pd
from minio.error import S3Error
from engine.Engine import Engine
from engine.EngineHelper import *
from io import BytesIO
from redis import Redis
from rq import Connection, Worker
from rq.queue import Queue
from rq.job import Job 

class RQPreparator(Engine):
    ADDR_COL_INDEX=6
    PUB_COLUMNS = ['title','type','quartile','citations','publisher','year','afil_id','dept_id']
    TEMP_FOLDERS={'mapped':'tmp_mapped','unmapped':'tmp_unmapped','failed':'failed'}
    TFM_WORK={'clean':'cln','postal_mapping':'psm','pattern_matching':'ptm','geocode':'gcd','institution_mapping':'inm','department_mapping':'dpm'}
    TFM_WAIT_TIME=2

    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_TRANSFORM']
        self.bucket = self.settings['MINIO_BUCKET_TRANSFORMED']
        self.previous_bucket = self.settings['MINIO_BUCKET_AGGREGATED']
        self.resources_bucket = self.settings['MINIO_BUCKET_RESOURCES']
        self.TEMP_FOLDERS['result']=self.settings['MINIO_RESULT_FOLDER']
        #dibikin suatu format di config?
        #misal RES_FILES = [{'id': 'region_standard', 'fname': 'region_standard.json'}] buat bantu loader di engine juga
        self.standard_region = self.settings['RES_FILES']['TFM_PTM_STD']
        self.standard_postal = self.settings['RES_FILES']['TFM_PSM_STD']
        self.standard_institution = self.settings['RES_FILES']['TFM_INM_STD']
        self.standard_department = self.settings['RES_FILES']['TFM_DPM_STD']
        self.rq_queue = [self.TFM_WORK['clean'],self.TFM_WORK['postal_mapping'],self.TFM_WORK['pattern_matching'],self.TFM_WORK['geocode'],self.TFM_WORK['institution_mapping'], self.TFM_WORK['department_mapping']]

    def _transform(self):
        logging.debug('Acquiring Lock for Transformation Jobs...')
        key, dimension, year = self._redis_update_stat_before(self.job)
        logging.debug('Transforming Records...')
        success, errormsg = self._transform_in_rq(dimension, year)
        #TO BE IMPLEMENTED
        #logging.debug('Do Geocoding...')
        #if success and not errormsg:
        #    success, errormsg = self._geocoding_in_rq(dimension, year)
        logging.debug('Updating Job Status...')
        self._redis_update_stat_after(key, self.job, success, errormsg)
        #FOR DEBUGGING
        #success, errormsg = self._transform_in_rq('ptn', 2018) #for debugging
        #success, errormsg = self._geocoding('ptn', 2018) #for debugging
        #print(success, errormsg)

    def _transform_in_rq(self, dimension, year):
        try:
            if self._check_dimension_source('PDKI', dimension):
                file_name=GenerateFileName(self.previous_bucket, dimension, year, 'csv')
                line_list = self._fetch_and_parse(self.previous_bucket, file_name, 'csv')
                
                #cleaning the data
                ll_cleaned = self._rq_cleaning(line_list, self.ADDR_COL_INDEX)

                #splitting the data based on postal code
                std_file = self._fetch_and_parse(self.resources_bucket, self.standard_postal, 'json')
                ll_mapped_postal, ll_unmapped = self._rq_split(ll_cleaned, std_file, self.TFM_WORK['postal_mapping'], self.ADDR_COL_INDEX)
                mapped_lines=LineListToLines(ll_mapped_postal)
                if self._is_all_mapped(mapped_lines, ll_unmapped, dimension, year): return True, True
                
                #splitting the data based on pattern matching
                std_file = self._fetch_and_parse(self.resources_bucket, self.standard_region, 'json')
                ll_mapped_pattern, ll_unmapped = self._rq_split(ll_unmapped, std_file, self.TFM_WORK['pattern_matching'],self.ADDR_COL_INDEX)
                mapped_lines=mapped_lines+LineListToLines(ll_mapped_pattern)
                if self._is_all_mapped(mapped_lines, ll_unmapped, dimension, year): return True, True
                
                unmapped_lines=LineListToLines(ll_unmapped)
                self._save_to_temp_folders(mapped_lines, unmapped_lines, dimension, year)
            elif self._check_dimension_source('SINTA', dimension):
                file_name=GenerateFileName(self.previous_bucket, dimension, year, 'csv', temp_folder='university', temp_prefolder=False)
                line_list = self._fetch_and_parse(self.previous_bucket, file_name, 'csv')

                #for university dataset, map institution to region and department to class
                std_file = self._fetch_and_parse(self.resources_bucket, self.standard_institution, 'json')
                ll_mapped = self._rq_map(line_list, std_file, self.TFM_WORK['institution_mapping'], self.PUB_COLUMNS, 'afil_id')
                std_file = self._fetch_and_parse(self.resources_bucket, self.standard_department, 'json')
                ll_mapped = self._rq_map(ll_mapped, std_file, self.TFM_WORK['department_mapping'], self.PUB_COLUMNS, 'dept_id')
                mapped_lines=LineListToLines(ll_mapped)

                #for non_university dataset, map institution to region and tile/journal to class
                file_name=GenerateFileName(self.previous_bucket, dimension, year, 'csv', temp_folder='non_university', temp_prefolder=False)
                line_list = self._fetch_and_parse(self.previous_bucket, file_name, 'csv')
                std_file = self._fetch_and_parse(self.resources_bucket, self.standard_institution, 'json')
                ll_unmapped = self._rq_map(line_list, std_file, self.TFM_WORK['institution_mapping'], self.PUB_COLUMNS[:-1], 'afil_id')
                #need to implement a pattern-matching algorithm using machine learning model to map title/journal to subject
                unmapped_lines=LineListToLines(ll_unmapped)

                self._save_to_temp_folders(mapped_lines, unmapped_lines, dimension, year)
                if self._is_all_mapped(mapped_lines, ll_unmapped, dimension, year): return True, True
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    def _is_all_mapped(self, mapped_lines, ll_unmapped, dimension, year):
        if not ll_unmapped:
            self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['result'])
            return True
        return False

    def _save_to_temp_folders(self, mapped_lines, unmapped_lines, dimension, year):
        if mapped_lines:
            self._save_data_to_minio(mapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['mapped'])
        if unmapped_lines:
            self._save_data_to_minio(unmapped_lines, self.bucket, dimension, year, temp_folder=self.TEMP_FOLDERS['unmapped'])

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
            'malaysia', 'kuching',
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
                    if job.get_status()=='finished':
                        if job.result: 
                            line_mapped, line_unmapped = job.result
                            ll_mapped.append(line_mapped) if line_mapped else ll_unmapped.append(line_unmapped)
                        job_id.remove(id)
            else:
                break
            time.sleep(self.TFM_WAIT_TIME)     
        return ll_mapped, ll_unmapped

    def _rq_map(self, line_list, std_file, tfm_work, columns, map_col):
        #disort dulu baru dimap pake mapreduce lebih masuk akal, mungkin postal mapping juga(?)
        job_id=None
        with Connection():
            if tfm_work==self.TFM_WORK['institution_mapping']:
                job = self.queue[tfm_work].enqueue(InstitutionMapping, args=(line_list, std_file, columns, map_col))
            elif tfm_work==self.TFM_WORK['department_mapping']:
                job = self.queue[tfm_work].enqueue(DepartmentMapping, args=(line_list, std_file, columns, map_col))
            job_id=job.id
        ll_mapped=None
        while True:
            job=Job(job_id, self.rq_conn)
            if job.get_status()=='finished':
                if job.result: 
                    ll_mapped = job.result
                break
        return ll_mapped

    def _geocoding_in_rq(self, dimension, year):
        #set api config
        API_CONFIG={}
        mapped_fname=GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.TEMP_FOLDERS['mapped'])
        unmapped_fname=GenerateFileName(self.bucket, dimension, year, 'csv', temp_folder=self.TEMP_FOLDERS['unmapped'])
        try:
            data_output = self._fetch_file_from_minio(self.bucket, unmapped_fname)
            ll_unmapped = BytesToLines(data_output, line_list=True) if data_output else None
            if not ll_unmapped: raise Exception('405: File Not Fetched')
            data_output = self._fetch_file_from_minio(self.resources_bucket, self.standard_region)
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
        logging.info("Starting Prearator Engine...")
        setup_rq = self._setup_rq(self.rq_queue)
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
        self._setup_rq(self.rq_queue)
        queues = [value for key, value in self.queue.items()]
        with Connection():
            worker = Worker(queues=queues, connection=self.rq_conn)
            worker.work()
            while True:
                if not worker.work():
                    worker.work()
        