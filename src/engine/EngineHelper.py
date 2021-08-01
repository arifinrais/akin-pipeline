#!/usr/bin/env python3
import json, sys, json, traceback, requests as req, pandas as pd #, time, csv, os, logging
from urllib.parse import quote
#import requests as req
from minio import Minio
from engine import config
from datetime import datetime
from rejson import Client, Path
from minio import Minio
from io import BytesIO, StringIO

def Scrape(req_item, dimension, year, minio_settings, file_id=None):
    try:
        BUCKET_NAME='raw'
        #harusnya MINIO_HOSTS tapi ada yang aneh dari dockernya
        MINIO_CLIENT = Minio(
            'minio'+':'+str(minio_settings['MINIO_PORT']),
            access_key=minio_settings['MINIO_ROOT_USER'],
            secret_key=minio_settings['MINIO_ROOT_PASSWORD'],
            secure=False
        )
        FILE_NAME=dimension+'/'+str(year)+'/'+BUCKET_NAME+'_'+dimension+'_'+str(year)
        if file_id:
            zero_prefix='00' if file_id<10 else '0' if file_id<100 else ''
            FILE_NAME+='_'+zero_prefix+str(file_id)
        resp=req.get(req_item['url'])
        num_of_records = 0
        if dimension == 'ptn' or dimension=='trd':
            FILE_NAME+='.json'
            resp_dict = resp.json()
            num_of_records = resp_dict['hits']['total']['value']
            content = json.dumps(resp_dict['hits'], ensure_ascii=False, indent=4).encode('utf-8') # convert dict to bytes
            _content_type='application/json'
        elif dimension=='pub':
            FILE_NAME+='.html'
            content=resp.text.encode('utf-8') #convert text/html to bytes for reverse conversion use bytes.decode()
            _content_type='text/html'
        resp.close()
        if num_of_records:
            result = MINIO_CLIENT.put_object(BUCKET_NAME, FILE_NAME, BytesIO(content), length=-1, part_size=5*1024*1024, content_type=_content_type) #assuming maximum json filesize 1MB, minimum 5MiB
            return result.object_name
        else:
            return '404: 0 number of records'
    except:
        emssg, b, c =sys.exc_info()
        return emssg

def GenerateFileName(bucket_base, dimension, year, extension, file_id=None):    
    if file_id:
        zero_prefix= '00' if file_id<10 else '0' if file_id <100 else ''
        _file_id = zero_prefix+str(file_id)
        return dimension+'/'+str(year)+'/'+bucket_base+'_'+dimension+'_'+str(year)+'_'+_file_id+extension       
    else:
        return dimension+'/'+bucket_base+'_'+dimension+'_'+str(year)+extension

def CreateCSVLine(fields, delimiter="\t", lineterminator='\n'):
    line = ""
    for i in range(len(fields)):
        if i<len(fields)-1:
            line+=str(fields[i])+delimiter
        else:
            line+=str(fields[i])+lineterminator
    return line

def BytesToDataFrame(databytes, fields, delimiter="\t", lineterminator='\n'):
    output = StringIO()
    output.write(CreateCSVLine(fields))
    output.write(databytes.decode('utf-8'))
    output.seek(0)
    df = pd.read_csv(output,delimiter=delimiter,lineterminator=lineterminator)
    return df


def ParseCSVLine(line, delimiter="\t"):
    return line.strip().split(delimiter)

#Handler and Logger        
def WrongInputHandler(err):
    exc_type, exc_value, exc_traceback = err
    trace_back = traceback.extract_tb(exc_traceback)
    print(" >Exception Type: %s" % exc_type.__name__)
    print(" >Exception Message: %s" % exc_value)
    print(" >Stack Trace:")
    for trace in trace_back:
        print("  >file: %s, line: %d, funcName: %s, message: %s" % (trace[0], trace[1], trace[2], trace[3]))

def ErrorHandler():
    #logging utils(?)
    None