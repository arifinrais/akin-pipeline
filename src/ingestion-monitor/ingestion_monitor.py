import sys
import config
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
            self.feedSchema = {
                "type": "object",
                "properties": {
                    "dimension": {"type": "string", "pattern": "^ptn|pub|trd|patent|publication|trademark$"},
                    "year": {"type": "integer", "minimum": self.settings['MIN_SCRAPE_YEAR'], "maximum": self.settings['MAX_SCRAPE_YEAR']}
                }
            }
        except:
            self.errorHandler(sys.exc_info())

    def feed(self, jsonObj):
        #feed to kafka (status fed) and redis (feed object)
        None

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
        #dengerin proses queue
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
    # Run command is used to monitor the ingestion process
    # Feed command is used to feed a context to be scraped
    ing_monitor=IngestionMonitor()
    try:
        command = sys.argv[1]
        if command=='feed':
            body = json.loads(sys.argv[2])
            validate(body, ing_monitor.feedSchema)
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

