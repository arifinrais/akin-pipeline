import sys
import config
import json
import traceback
from jsonschema import validate

class Aggregator:

    def __init__(self):
        # Load settings from local config.py file
        None

def main():
    #wait loop
        #if lock redis agg_statDB
            #copy the json body of a key in redis agg_statDB (anykey, {dimension, year, status, timestamp, errormsg})
            #update the copied key in redis agg_statDB (anykey, {dimension, year, status, timestamp, errormsg})
            #add/update the key in redis transformDB (hostname, {dimension, year})
            #unlock redis agg_statDB
            
            #aggregate based on the key
            #if success
                #save to minio/agg/<dimension>/agg_<dim_code>_<year>.<csv/avro>
                #add a key in redis tfm_statDB(anykey, {dimension, year, status, timestamp, errormsg})
            #else
                #add errormsg
            #update the copied key in redis agg_statDB (anykey, {dimension, year, status, timestamp, errormsg}) error/not
        #else
            #sleep in configured seconds
    None

if __name__ == "__main__":
    sys.exit(main())

