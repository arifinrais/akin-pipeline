import sys
import config
import json
import traceback
from jsonschema import validate

class JobstatConnector:

    def __init__(self):
        # Load settings from local config.py file
        None

def main():
    #wait loop
        #read key value pair from redis aggstatDB, tfmstatDB, anlstatDB

        #retry if KafkaProducer
            #send formatted stat: ingested, transformed, analyzed, failed

        #sleep in configured seconds
    None

if __name__ == "__main__":
    sys.exit(main())

