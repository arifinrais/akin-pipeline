import sys
import config
import json
import traceback
import time
from kafka import KafkaProducer
from tenacity import retry
from jsonschema import validate

class JobstatHub:

    def __init__(self):
        try:
            configs=[item for item in dir(config) if not item.startswith("__")]
            self.settings={}
            for item in configs:
                self.settings[item]=getattr(config, item)
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
    
    def run(self):
        while True:
            #read key value pair from redis aggstatDB, tfmstatDB, anlstatDB

            #retry if KafkaProducer
                #send formatted stat: ingested, transformed, analyzed, failed

            time.sleep(self.settings['SLEEP_TIME'])

    @staticmethod
    def error_handler():
        #logging utils(?)
        None 

def main():
    hub = JobstatHub()
    hub.run()

if __name__ == "__main__":
    sys.exit(main())

