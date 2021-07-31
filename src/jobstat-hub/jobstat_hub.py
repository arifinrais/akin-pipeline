#!/usr/bin/env python3
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from rejson import Client, Path
import config as cfg
import sys, logging, json, traceback, time
#from tenacity import retry
#from jsonschema import validate

class JobstatHub(object):
    def __init__(self):
        try:
            '''
            configs=[item for item in dir(config) if not item.startswith("__")]
            self.settings={}
            for item in configs:
                self.settings[item]=getattr(config, item)
            self.all_locks=[cfg.LOCK_INGEST,cfg.LOCK_AGGREGATE,cfg.LOCK_TRANSFORM]
            '''
            self.all_locks=[cfg.LOCK_INGEST,cfg.LOCK_AGGREGATE,cfg.LOCK_ANALYZE]
            #print(self.all_locks)
            self.jobstat_schema={
                "type": "object",
                "properties": {
                    "timestamp": {"type": "date-time"},
                    "patent": {
                        "type": "array",
                        "years": {
                            "type": "object",
                            "properties": {
                                "year": {"type": "integer", "minimum": cfg.MIN_INGEST_YEAR, "maximum": cfg.MAX_INGEST_YEAR},
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
                                "year": {"type": "integer", "minimum": cfg.MAX_INGEST_YEAR, "maximum": cfg.MAX_INGEST_YEAR},
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
                                "year": {"type": "integer", "minimum": cfg.MIN_INGEST_YEAR, "maximum": cfg.MAX_INGEST_YEAR},
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
            self.redis_conn = Client(host=cfg.JOB_REDIS_HOST, 
                                port=cfg.JOB_REDIS_PORT, 
                                password=cfg.JOB_REDIS_PASSWORD,
                                db=cfg.JOB_REDIS_DB,
                                decode_responses=True,
                                socket_timeout=cfg.JOB_REDIS_SOCKET_TIMEOUT,
                                socket_connect_timeout=cfg.JOB_REDIS_SOCKET_TIMEOUT)
            return True
        except:
            return False

    def _setup_kafka_producer(self):
        brokers = cfg.KAFKA_HOSTS
        try:
            self.kafka_producer = KafkaProducer(bootstrap_servers=cfg.KAFKA_HOSTS,
                                    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                                    retries=3,
                                    linger_ms=cfg.KAFKA_PRODUCER_BATCH_LINGER_MS,
                                    buffer_memory=cfg.KAFKA_PRODUCER_BUFFER_BYTES)
            return True
        except:
            print(sys.exc_info())
            return False

    #@retry
    def _feed_job_stats_to_kafka(self, job_stats, retry_limit=10):
        failed_count=0
        while True:
            if self.kafka_producer is not None:
                try:
                    self.kafka_producer.send(cfg.KAFKA_INCOMING_TOPIC,job_stats)
                    self.kafka_producer.flush()
                except KafkaTimeoutError as e:
                    logging.error(e)
                    time.sleep(2)
                    failed_count+=1
                    if failed_count==retry_limit: return False
                    continue
                return True
            else:
                self._setup_kafka_producer()
                time.sleep(2)

    def _read_job_stats(self):
        job_stats={}
        job_stats['timestamp']=datetime.utcnow().isoformat()
        job_stats['patent']=[];job_stats['trademark']=[];job_stats['publication']=[]
        for _key in self.redis_conn.scan_iter(match='[pt][rtu][bdn]_[0-9][0-9][0-9][0-9]',count=100): #match="[pt][rtu][bdn]_[0-9][0-9][0-9][0-9]"
            if _key in self.all_locks: continue
            try: #ini bingung kadang kalau gapake json.loads gakebaca, tapi kalau gaada kadang TypeError
                _job=json.loads(self.redis_conn.jsonget(_key, Path('.')))
            except TypeError:
                _job=self.redis_conn.jsonget(_key, Path('.'))
            job_stat = {}
            job_stat['year']=_job['year']
            job_stat['job']=_job['job']
            job_stat['status']=_job['status']
            job_stat['timestamp']=_job['timestamp']
            job_stat['errormsg']=_job['errormsg']
            if _job['dimension']==cfg.DIMENSION_PATENT: job_stats['patent'].append(job_stat)
            if _job['dimension']==cfg.DIMENSION_PUBLICATION: job_stats['publication'].append(job_stat)
            if _job['dimension']==cfg.DIMENSION_TRADEMARK: job_stats['trademark'].append(job_stat)
        return job_stats

    def run(self):
        setup_redis = self._setup_redis_conn()
        setup_kafka = self._setup_kafka_producer()
        logging.info("Jobstat Hub Successfully Started") if setup_kafka and setup_redis else logging.warning("Problem in Starting Jobstat Hub")
        while True:
            job_stats = self._read_job_stats()
            test=self._feed_job_stats_to_kafka(job_stats)
            print(test)
            time.sleep(5*60)

    def close(self):
        # Properly exiting the application
        if self.kafka_producer is not None:
            self.kafka_producer.close()

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
    logging.basicConfig(filename='jobstat.log', encoding='utf-8', level=logging.DEBUG) # for production WARNING
    try:
        hub = JobstatHub()
        hub.run()
    except KeyboardInterrupt:
        print("Closing Ingestion Monitor...")
        hub.close()
    except:
        hub.error_handler(sys.exc_info())

if __name__ == "__main__":
    sys.exit(main())

'''
#@retry(wait_exponential_multiplier=500, wait_exponential_max=10000)
def _create_producer(self):
    """Tries to establish a Kafka consumer connection"""
    try:
        brokers = cfg.KAFKA_HOSTS
        self.logger.debug("Creating new kafka producer using brokers: " +
                            str(brokers))

        return KafkaProducer(bootstrap_servers=brokers,
                                value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                                retries=3,
                                linger_ms=cfg.KAFKA_PRODUCER_BATCH_LINGER_MS,
                                buffer_memory=cfg.KAFKA_PRODUCER_BUFFER_BYTES)
    except KeyError as e:
        self.logger.error('Missing setting named ' + str(e),
                            {'ex': traceback.format_exc()})
    except:
        self.logger.error("Couldn't initialize kafka producer.",
                            {'ex': traceback.format_exc()})
        raise
'''
'''
#for reference to kafka producer
def feed(self, json_item):
    #@MethodTimer.timeout(cfg.KAFKA_FEED_TIMEOUT, False)
    def _feed(json_item):
        producer = self._create_producer()
        topic = cfg.KAFKA_INCOMING_TOPIC
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
'''


