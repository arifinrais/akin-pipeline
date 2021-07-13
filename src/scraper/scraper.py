import sys
import config
import json
import traceback
from jsonschema import validate

class Scraper:

    def __init__(self):
        # Load settings from local config.py file
        None

def main():
    #wait loop
        #if lock redis queueDB
            #copy the json body of a key in redis queueDB (anykey, {dimension, year})
            #delete the copied key in redis queueDB
            #add/update the key in redis ingestDB (hostname, {dimension, year})
            #unlock redis queueDB
            
            #scrape based on the key
            #if success
                #save to minio/raw/<dimension>/<year>/raw_<dim_code>_<year>_<#>.<json/html>
                #add a key in another_redis agg_statDB(anykey, {dimension, year, status, timestamp, errormsg})
            #else
                #add a key in redis errorDB (anykey, {dimension, year, hostname, timestamp, errormsg})
        #else
            #sleep in configured seconds
    None

if __name__ == "__main__":
    sys.exit(main())

