#!/usr/bin/env python3
import sys, time
import logging
from engine.Engine import Engine
from engine.EngineHelper import Scrape, GetPages
from redis import Redis
from rq import Connection, Worker
from rq.queue import Queue
from rq.job import Job 

class Ingestor(Engine):
    def __init__(self, scrape_wait_time=5):
        Engine.__init__(self)
        self.job = self.settings['JOB_INGEST']
        self.bucket = self.settings['MINIO_BUCKET_INGESTED']
        self.scrape_wait_time = scrape_wait_time
        self.rq_queue = [self.settings['DIMENSION_PATENT'],self.settings['DIMENSION_TRADEMARK'],self.settings['DIMENSION_PUBLICATION']]

    def _ingest(self):
        logging.debug('Acquiring Lock for Ingestion Jobs...')
        key, dimension, year = self._redis_update_stat_before(self.job)
        logging.debug('Ingesting Records...')
        success, errormsg = self._ingest_records(dimension, year)
        logging.debug('Updating Job Status...')
        self._redis_update_stat_after(key, self.job, success, errormsg)

    def _ingest_records(self, dimension, year):
        try:
            minio_settings=self._get_minio_settings()
            req_list = self._generate_req_list(dimension, year)
            job_id = []; file_id = 1
            for req_item in req_list:
                with Connection():
                    job = self.queue[dimension].enqueue(Scrape, args=(req_item, dimension, year, minio_settings, file_id))
                    job_id.append(job.id)
                    file_id+=1
            self._wait_for_job_to_finish(job_id)
            return True, None
        except:
            # in the meantime error message is just its value, log
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    def _get_minio_settings(self):
        parameters = ['MINIO_HOST', 'MINIO_PORT', 'MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD']
        minio_settings={}
        for param in parameters: minio_settings[param]=self.settings[param]
        return minio_settings

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
                    #req_item['header']= self._generate_header(dimension)
                    req_list.append(req_item)
        elif self._check_dimension_source('SINTA', dimension):
            ll_hit =  self._fetch_and_parse('res','pub_hit_list.txt',delimiter='_')
            minio_settings=self._get_minio_settings()
            job_id = []
            for line in ll_hit:
                if len(line)==2:
                    with Connection():
                        req_item={'url':'https://sinta.ristekbrin.go.id/departments/detail',
                            'params': {"afil":line[-1],"id":line[0],"view":"documentsscopus"},
                            'afil_type': 'uni'}
                        job = self.queue[dimension].enqueue(GetPages, args=(req_item, dimension, year, minio_settings))
                        job_id.append(job.id)
                elif len(line)==1:
                    with Connection():
                        req_item={'url':'https://sinta.ristekbrin.go.id/affiliations/detail',
                            'params': {"id":line[0],"view":"documentsscopus"},
                            'afil_type': 'non_uni'}
                        job = self.queue[dimension].enqueue(GetPages, args=(req_item, dimension, year, minio_settings))
                        job_id.append(job.id)
            ll_getpages = self._get_pages_result(job_id)
            for line in ll_getpages:
                pages=line[-1]
                for i in range(int(pages)):
                    req_item={}
                    if len(line)==3:
                        req_item={'url':'https://sinta.ristekbrin.go.id/departments/detail',
                            'params': {"afil":line[-1],"id":line[0],"view":"documentsscopus","pages": str(i+1)},
                            'afil_type': 'uni'}
                    elif len(line)==2:
                        req_item={'url':'https://sinta.ristekbrin.go.id/affiliations/detail',
                            'params': {"id":line[0],"view":"documentsscopus","pages": str(i+1)},
                            'afil_type': 'non_uni'}
                    if req_item:
                        #req_item['header']=self._generate_header(dimension)
                        req_list.append(req_item)
        return req_list
    
    def _get_pages_result(self, job_id):
        ll_getpages=[]
        while True:
            if len(job_id):
                for id in job_id:
                    job = Job(id, self.rq_conn)
                    if job.get_status()=='finished':
                        if job.result: 
                            temp=job.result.split('_')
                            if len(temp)>1:
                                ll_getpages.append(temp)
                        job_id.remove(id)
            else:
                break
        #time.sleep(self.settings['SL'])     
        return ll_getpages
        
    def _generate_parameters(self, dimension, year):
        if self._check_dimension_source('PDKI', dimension):
            param_type,param_keywords,param_dates='',[],[]
            _year=str(year)
            if dimension==self.settings['DIMENSION_PATENT']:
                param_type='patent'
                param_keywords=['PID','P00','S00','W00','P22']
            elif dimension==self.settings['DIMENSION_TRADEMARK']:
                param_type='trademark'
                param_keywords=['DID','D00','J00','K00','M00','R00','V00','D22']
            param_keywords = [keyword+_year for keyword in param_keywords]
            month_28, month_30, month_31 =[2], [4,6,9,11], [1,3,5,7,8,10,12]
            for i in range(12):
                _month=str(i+1)
                if i+1<10: _month='0'+_month
                date_base=_year+'-'+_month+'-'
                if i+1 in month_28: param_dates.append([date_base+'01',date_base+'28'])
                if i+1 in month_30: param_dates.append([date_base+'01',date_base+'30'])
                if i+1 in month_31: param_dates.append([date_base+'01',date_base+'31'])
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

    def _wait_for_job_to_finish(self, job_ids):
        while True:
            job_done = True
            for id in job_ids:
                job = Job.fetch(id, self.rq_conn)
                print(job.result) #just checkin'
                if job.get_status()!='finished':
                    job_done=False
                    break
            if job_done:
                break
            time.sleep(self.scrape_wait_time)
            print("still doing the job") # just checkin'
        print("the jobs are done") #add to logging
        return

    def start(self):
        logging.info("Starting Ingestion Engine...")
        setup_rq = self._setup_rq(self.rq_queue)
        setup_redis = self._setup_redis_conn()
        setup_minio = self._setup_minio_client(self.bucket)
        logging.info("Ingestion Engine Successfully Started") if setup_rq and setup_redis and setup_minio else logging.warning("Problem in Starting Ingestion Engine")
        while True:
            self._ingest()
            time.sleep(self.settings['SLEEP_TIME'])

    def scrape(self):
        self._setup_rq(self.rq_queue)
        queues = [value for key, value in self.queue.items()]
        with Connection():
            worker = Worker(queues=queues, connection=self.rq_conn)
            worker.work()
            while True:
                if not worker.work():
                    worker.work()

