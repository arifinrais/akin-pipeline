import sys
import config
import json
import traceback

def errorHandler(err):
    exc_type, exc_value, exc_traceback = err
    trace_back = traceback.extract_tb(exc_traceback)
    print("Exception Type: %s" % exc_type.__name__)
    print("Exception Message: %s" % exc_value)
    print("Stack Trace:")
    for trace in trace_back:
        print("  >>file: %s, line: %d, funcName: %s, message: %s" % (trace[0], trace[1], trace[2], trace[3]))

class IngestionMonitor:
    consumer = None

    def __init__(self):
        configs=[item for item in dir(config) if not item.startswith("__")]
        self.settings={}
        for item in configs:
            self.settings[item]=getattr(config, item)

    def feed(self, jsonObj):
        #feed to kafka and redis
        None

    def run(self):
        self._setup_consumer()
        print("Successfully connected to Kafka")
        self._main_loop()

    def close(self):
        if self.consumer is not None:
            self.consumer.close()

    def _setup_consumer(self):
        #set up consumer dengerin topic ingestion_stat
        None
    
    def _main_loop(self):
        #listen to kafka topic ingestion_stat
        None
    
def main():
    ing_monitor=IngestionMonitor()
    try:
        command = sys.argv[1]
        if command=='feed':
            body = json.loads(sys.argv[2])
            if body['dim'] and body['year']:
                if (body['dim'] in ["ptn", "trd", "pub"]):
                    ing_monitor.feed(body)
                else:
                    raise ValueError
        elif command=='run':
            ing_monitor.run()
        else:
            raise ValueError
    except IndexError:
        errorHandler(sys.exc_info())
    except json.decoder.JSONDecodeError:
        errorHandler(sys.exc_info())
    except ValueError:
        errorHandler(sys.exc_info())
    except KeyError:
        errorHandler(sys.exc_info())
    except KeyboardInterrupt:
        print("Closing Ingestion Monitor...")
        ing_monitor.close()

if __name__ == "__main__":
    sys.exit(main())

