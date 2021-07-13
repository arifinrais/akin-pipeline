import sys
import config
import json
import traceback
from jsonschema import validate

class Analytics:

    def __init__(self):
        # Load settings from local config.py file
        None

def main():
    #wait loop
        #if lock redis anl_statDB
            #copy the json body of a key in redis anl_statDB (anykey, {dimension, year, status, timestamp, errormsg})
            #update the copied key in redis anl_statDB (anykey, {dimension, year, status, timestamp, errormsg})
            #unlock redis anl_statDB
            
            #analyze the data
            #if success
                #save to mongodb
            #else
                #add errormsg
            #update the copied key in redis anl_statDB (anykey, {dimension, year, status, timestamp, errormsg}) error/not
        #else
            #sleep in configured seconds
    None

if __name__ == "__main__":
    sys.exit(main())

