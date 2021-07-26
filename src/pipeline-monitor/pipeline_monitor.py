import sys
import localsettings as config
import json
import traceback
from jsonschema import validate

class IngestionMonitor:
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
            self.errorHandler(sys.exc_info())

    def feed(self, jsonObj):
        #feed to kafka (status fed) and redis (feed object)
        '''
        HARUSNYA KAFKA STATUS NESTED DI REDIS ABIS FEED OBJECT ATAU BENERAN JADIIN WEBAPP AJA
        @MethodTimer.timeout(self.settings['KAFKA_FEED_TIMEOUT'], False)
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
        '''

    def run(self):
        self._setup_consumer()
        print("Successfully connected to Kafka")
        self._setup_qmonitor()
        self._main_loop()

    def close(self):
        # Properly exiting the application
        if self.consumer is not None:
            self.consumer.close()

    def _setup_consumer(self):
        #set up consumer dengerin topic ingestion_stat
        None

    def _setup_qmonitor(self):
        #dengerin queue_stat
        None
    
    def _main_loop(self):
        #listen to kafka topic ingestion_stat
        None
    
    @staticmethod
    def errorHandler(err):
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
    ing_monitor=IngestionMonitor()
    try:
        command = sys.argv[1]
        if command=='feed':
            body = json.loads(sys.argv[2])
            validate(body, ing_monitor.schemas['FEED'])
        elif command=='run':
            ing_monitor.run()
        else:
            raise ValueError
    except KeyboardInterrupt:
        print("Closing Ingestion Monitor...")
        ing_monitor.close()
    except:
        IngestionMonitor.errorHandler(sys.exc_info())

if __name__ == "__main__":
    sys.exit(main())

