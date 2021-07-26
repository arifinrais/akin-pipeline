import sys
import localsettings as config
import json
import traceback
import time
from rejson import Client, Path
from kafka import KafkaConsumer
from jsonschema import validate
from datetime import datetime

class PipelineMonitor:
    # KafkaConsumer instance
    consumer = None

    def __init__(self):
        # Load settings from local config.py file
        try:
            configs=[item for item in dir(config) if not item.startswith("__")]
            self.settings={}
            for item in configs:
                self.settings[item]=getattr(config, item)
            self.schemas={}
            self.schemas['FEED'] = {
                "type": "object",
                "properties": {
                    "dimension": {"type": "string", "pattern": "^ptn|pub|trd|patent|publication|trademark$"},
                    "year": {"type": "integer", "minimum": self.settings['MIN_SCRAPE_YEAR'], "maximum": self.settings['MAX_SCRAPE_YEAR']}
                }
            }
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
            self.schemas['JOBSTAT']={
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

    def feed(self, feed_obj):
        self._setup_redis_conn()
        _object = {}
        _object['dimension'] = self._get_dimension(feed_obj['dimension'])
        _object['year'] = feed_obj['year']
        _object['job'] = self.settings['JOB_INGEST']
        _object['status'] = self.settings['STAT_WAIT']
        _object['timestamp'] = datetime.utcnow().isoformat()
        _object['errormsg'] = ''
        _key = self._get_dimension(feed_obj['dimension']) + '_' + str(feed_obj['year'])
        self.redis_conn.jsonset(_key, Path.rootPath(), json.dumps(_object))
        
    def run(self):
        self.consumer = self._setup_consumer()
        self._main_loop()

    def close(self):
        # Properly exiting the application
        if self.consumer is not None:
            self.consumer.close()

    def _get_dimension(self, dimension):
        if dimension == 'ptn' or dimension == 'patent': return 'ptn'
        if dimension == 'trd' or dimension == 'trademark': return 'trd'
        if dimension == 'pub' or dimension == 'publication': return 'pub'

    def _setup_consumer(self):
        try:
            broker = self.settings['KAFKA_HOST']+':'+str(self.settings['KAFKA_PORT'])
            return KafkaConsumer(
                self.settings['KAFKA_INCOMING_TOPIC'],
                group_id=self.settings['KAFKA_GROUP'],
                bootstrap_servers=broker,
                value_deserializer=lambda m: m.decode('utf-8'),
                consumer_timeout_ms=self.settings['KAFKA_CONSUMER_TIMEOUT'],
                auto_offset_reset=self.settings['KAFKA_CONSUMER_AUTO_OFFSET_RESET'],
                auto_commit_interval_ms=self.settings['KAFKA_CONSUMER_COMMIT_INTERVAL_MS'],
                enable_auto_commit=self.settings['KAFKA_CONSUMER_AUTO_COMMIT_ENABLE'],
                max_partition_fetch_bytes=self.settings['KAFKA_CONSUMER_FETCH_MESSAGE_MAX_BYTES'])
        except:
            PipelineMonitor.error_handler(sys.exc_info())

    def _setup_redis_conn(self):
        self.redis_conn = Client(host=self.settings['JOB_REDIS_HOST'], 
                            port=self.settings['JOB_REDIS_PORT'], 
                            password=self.settings['JOB_REDIS_PASSWORD'],
                            db=self.settings['JOB_REDIS_DB'],
                            decode_responses=True,
                            socket_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'],
                            socket_connect_timeout=self.settings['JOB_REDIS_SOCKET_TIMEOUT'])
    
    def _main_loop(self):
        while True:
            # read all job_stat from Kafka
            time.sleep(10)#self.settings['SLEEP_TIME'])
    
    @staticmethod
    def error_handler(err):
        # Customized error handler, can be used for log
        exc_type, exc_value, exc_traceback = err
        trace_back = traceback.extract_tb(exc_traceback)
        print(" >Exception Type: %s" % exc_type.__name__)
        print(" >Exception Message: %s" % exc_value)
        print(" >Stack Trace:")
        for trace in trace_back:
            print("  >file: %s, line: %d, funcName: %s, message: %s" % (trace[0], trace[1], trace[2], trace[3]))
    
def main():
    # Handle two types of command: run and feed
    # Run command is used to monitor jobs execution
    # Feed command is used to feed a context to be ingested
    pipeline_monitor=PipelineMonitor()
    try:
        command = sys.argv[1]
        if command=='feed':
            body = json.loads(sys.argv[2])
            validate(body, pipeline_monitor.schemas['FEED'])
            pipeline_monitor.feed(body)
        elif command=='run':
            pipeline_monitor.run()
        else:
            raise ValueError
    except KeyboardInterrupt:
        print("Closing Ingestion Monitor...")
        pipeline_monitor.close()
    except:
        PipelineMonitor.error_handler(sys.exc_info())

if __name__ == "__main__":
    sys.exit(main())

