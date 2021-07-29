#!/usr/bin/env python3
import sys, time
import logging
from engine.Engine import Engine
from engine.EngineHelper import Scrape
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

    def _setup_rq(self):
        self.rq_conn = Redis(host=self.settings['RQ_REDIS_HOST'], 
                            port=self.settings['RQ_REDIS_PORT'], 
                            password=self.settings['RQ_REDIS_PASSWORD'],
                            db=self.settings['RQ_REDIS_DB'],
                            decode_responses=False, #koentji
                            socket_timeout=self.settings['RQ_REDIS_SOCKET_TIMEOUT'],
                            socket_connect_timeout=self.settings['RQ_REDIS_SOCKET_TIMEOUT'])
        self.queue={}
        self.queue[self.settings['DIMENSION_PATENT']] = Queue(self.settings['DIMENSION_PATENT'], connection=self.rq_conn)
        self.queue[self.settings['DIMENSION_TRADEMARK']]= Queue(self.settings['DIMENSION_TRADEMARK'], connection=self.rq_conn)
        self.queue[self.settings['DIMENSION_PUBLICATION']] = Queue(self.settings['DIMENSION_PUBLICATION'], connection=self.rq_conn)
    
    def _ingest(self):
        key, dimension, year = self._redis_update_stat_before(self.job)
        success, errormsg = self._ingest_records(dimension, year)
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
                    req_item['header']= self._generate_header(dimension)
                    req_list.append(req_item)
        return req_list

    def _generate_parameters(self, dimension, year):
        if self._check_dimension_source('PDKI', dimension):
            param_type=''
            param_keywords=[]
            param_dates=[]
            _year=str(year)
            if dimension==self.settings['DIMENSION_PATENT']:
                param_type='patent'
                param_keywords=['PID','P00','S00','W00']
            elif dimension==self.settings['DIMENSION_TRADEMARK']:
                param_type='trademark'
                param_keywords=['DID','D00','J00','K00','M00','R00','V00']
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
        self._setup_rq()
        self._setup_redis_conn()
        self._setup_minio_client(self.bucket)
        while True:
            self._ingest()
            time.sleep(self.settings['SLEEP_TIME'])

    def scrape(self):
        self._setup_rq()
        #logging.getLogger().setLevel(logging.ERROR) #to prevent the scraper showing warning, debug, and info messages
        with Connection():
            queues=[self.queue[self.settings['DIMENSION_PATENT']],self.queue[self.settings['DIMENSION_TRADEMARK']],self.queue[self.settings['DIMENSION_PUBLICATION']]]
            worker = Worker(queues=queues, connection=self.rq_conn)
            worker.work()
            while True:
                if not worker.work():
                    worker.work()

