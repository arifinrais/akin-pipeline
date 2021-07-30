#!/usr/bin/env python3

import sys
import config
import json
import traceback
import time
from jobstat_hub import config
from kafka import KafkaProducer
from tenacity import retry
from jsonschema import validate
from rejson import Client, Path

class JobstatHub(object):

    def __init__(self):
        try:
            configs=[item for item in dir(config) if not item.startswith("__")]
            self.settings={}
            for item in configs:
                self.settings[item]=getattr(config, item)
            self.all_locks=[self.settings['LOCK_INGEST'],self.settings['LOCK_AGGREGATE'],self.settings['LOCK_TRANSFORM']]
            self.jobstat_schema={
                "type": "object",
                "properties": {
                    "timestamp": {"type": "date-time"},
                    "patent": {
                        "type": "array",
                        "years": {
                            "type": "object",
                            "properties": {
                                "year": {"type": "integer", "minimum": self.settings['MIN_SCRAPE_YEAR'], "maximum": self.settings['MAX_SCRAPE_YEAR']},
                                "job": {"type": "string", "pattern": "^agg|tfm|anl$"},
                                "status": {"type": "string", "pattern": "^wait|wip|done|err$"},
                                "timestamp": {"type": "date-time"},
                                "errormsg": {"type": "string"}
                            }
                        }
                    },
                    "trademark": {
                        "type": "array",
                        "years": {
                            "type": "object",
                            "properties": {
                                "year": {"type": "integer", "minimum": self.settings['MIN_SCRAPE_YEAR'], "maximum": self.settings['MAX_SCRAPE_YEAR']},
                                "job": {"type": "string", "pattern": "^agg|tfm|anl$"},
                                "status": {"type": "string", "pattern": "^wait|wip|done|err$"},
                                "timestamp": {"type": "date-time"},
                                "errormsg": {"type": "string"}
                            }
                        }
                    },
                    "publication": {
                        "type": "array",
                        "years": {
                            "type": "object",
                            "properties": {
                                "year": {"type": "integer", "minimum": self.settings['MIN_SCRAPE_YEAR'], "maximum": self.settings['MAX_SCRAPE_YEAR']},
                                "job": {"type": "string", "pattern": "^agg|tfm|anl$"},
                                "status": {"type": "string", "pattern": "^wait|wip|done|err$"},
                                "timestamp": {"type": "date-time"},
                                "errormsg": {"type": "string"}
                            }
                        }
                    }
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

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000)
    def _create_producer(self):
        """Tries to establish a Kafka consumer connection"""
        try:
            brokers = self.settings['KAFKA_HOSTS']
            self.logger.debug("Creating new kafka producer using brokers: " +
                               str(brokers))

            return KafkaProducer(bootstrap_servers=brokers,
                                 value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                                 retries=3,
                                 linger_ms=self.settings['KAFKA_PRODUCER_BATCH_LINGER_MS'],
                                 buffer_memory=self.settings['KAFKA_PRODUCER_BUFFER_BYTES'])
        except KeyError as e:
            self.logger.error('Missing setting named ' + str(e),
                               {'ex': traceback.format_exc()})
        except:
            self.logger.error("Couldn't initialize kafka producer.",
                               {'ex': traceback.format_exc()})
            raise

    #for reference to kafka producer
    def feed(self, json_item):
        '''
        Feeds a json item into the Kafka topic

        @param json_item: The loaded json object
        '''
        #@MethodTimer.timeout(self.settings['KAFKA_FEED_TIMEOUT'], False)
        def _feed(json_item):
            producer = self._create_producer()
            topic = self.settings['KAFKA_INCOMING_TOPIC']
            if not self.logger.json:
                self.logger.info('Feeding JSON into {0}\n{1}'.format(
                    topic, json.dumps(json_item, indent=4)))
            else:
                self.logger.info('Feeding JSON into {0}\n'.format(topic),
                                 extra={'value': json_item})

            if producer is not None:
                producer.send(topic, json_item)
                producer.flush()
                producer.close(timeout=10)
                return True
            else:
                return False

        result = _feed(json_item)

        if result:
            self.logger.info("Successfully fed item to Kafka")
        else:
            self.logger.error("Failed to feed item into Kafka")
    
    def _read_job_stats(self):
        job_stats={}
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

    def run(self):
        self._setup_redis_conn()
        while True:
            job_stats = self._read_job_stats()

            #read key value pair from redis aggstatDB, tfmstatDB, anlstatDB

            #retry if KafkaProducer
                #send formatted stat: ingested, transformed, analyzed, failed

            #time.sleep(self.settings['SLEEP_TIME'])
            time.sleep(5)

    @staticmethod
    def error_handler(err):
        #logging utils(?)
        exc_type, exc_value, exc_traceback = err
        trace_back = traceback.extract_tb(exc_traceback)
        print(" >Exception Type: %s" % exc_type.__name__)
        print(" >Exception Message: %s" % exc_value)
        print(" >Stack Trace:")
        for trace in trace_back:
            print("  >file: %s, line: %d, funcName: %s, message: %s" % (trace[0], trace[1], trace[2], trace[3]))

def main():
    hub = JobstatHub()
    hub.run()

if __name__ == "__main__":
    sys.exit(main())

