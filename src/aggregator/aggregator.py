import sys

import redis
import config
import json
import traceback
from redis import Redis
from tenacity import retry
from jsonschema import validate
import redis_lock
import time


class Aggregator:
    def __init__(self):
        # Load settings from local config.py file
        try:
            configs=[item for item in dir(config) if not item.startswith("__")]
            self.settings={}
            for item in configs:
                self.settings[item]=getattr(config, item)
            self.schemas={}
            self.schemas['JOB'] = {
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
            self.errorHandler(sys.exc_info())
        None
    
    def aggregate(self):
        self._setup_redis_conn()
        self._redis_update_stat()
        None

    def _setup_redis_conn(self):
        self.redis_conn = Redis(host=self.settings['JOB_REDIS_HOST'], 
                            port=self.settings['JOB_REDIS_PORT'], 
                            password=self.settings['JOB_REDIS_PASSWORD'],
                            db=self.settings['JOB_REDIS_DB'],
                            decode_responses=True,
                            socket_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'],
                            socket_connect_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'])
    #@retry
    def _redis_update_stat(self):
        lock = redis_lock.Lock(self.redis_conn, "aggregate_lock")
        key = ''
        dimension=''
        year=0
        while True:
            if lock.acquire(blocking=False):
                for _key in self.redis_conn.scan_iter():
                    temp=self.redis_conn.get(_key)
                    if temp['job']=="agg" and temp['status']=="wait":
                        temp = self.redis_conn.get(_key)
                        temp['status'] = 'wip' #update job status
                        key = _key
                        dimension=temp['dimension']
                        year=temp['year']
                        self.redis_conn.set(_key, temp)
                        break
                lock.release()
                break
            else:
                time.sleep(self.settings['SLEEP_TIME'])
        aggregated, errormsg = self._aggregate_records()
        while True:
            if lock.acquire(blocking=False):
                if aggregated:
                    temp = self.redis_conn.get(key)
                    temp['job'] = 'tfm'
                    temp['status'] = 'wait'
                    self.redis_conn.set(key, temp)
                else:
                    temp = self.redis_conn.get(key)
                    temp['job'] = 'agg'
                    temp['status'] = 'err'
                    temp['errormsg'] = errormsg
                    self.redis_conn.set(key, temp)
                lock.release()
                break
            else:
                time.sleep(self.settings['SLEEP_TIME'])

    def _aggregate_records(self):
        #try aggregate

            #return True, None
        #except
            #return False, errormsg



#JOB_REDIS_DB_AGG = 0
#JOB_REDIS_DB_TFM = 1
#JOB_REDIS_DB_ANL = 2
        None

def main():
    #wait loop
        #if lock redis agg_statDB
            #copy the json body of a key in redis agg_statDB (anykey, {dimension, year, job, status, timestamp, errormsg})
            #update the copied key in redis agg_statDB (anykey, {dimension, year, job, status, timestamp, errormsg})
            #add/update the key in redis tfm_statDB (anykey, {dimension, year, job, status, timestamp, errormsg})
            #unlock redis agg_statDB
            
            #aggregate based on the key
            #if success
                #save to minio/agg/<dimension>/agg_<dim_code>_<year>.<csv/avro>
                #add a key in redis tfm_statDB(anykey, {dimension, year, job, status, timestamp, errormsg})
            #else
                #add errormsg
            #update the copied key in redis agg_statDB (anykey, {dimension, year, job, status, timestamp, errormsg}) error/not
        #else
            #sleep in configured seconds
    None

if __name__ == "__main__":
    sys.exit(main())

