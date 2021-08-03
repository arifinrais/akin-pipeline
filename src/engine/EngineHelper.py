#!/usr/bin/env python3
import json, sys, json, traceback, requests as req, pandas as pd, regex as re #, time, csv, os, logging
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

def CleanAddress(line, regexp_list, country_list, col_idx=6):
    for regex_pair in regexp_list:
        rgxpattern, replacement = regex_pair
        line[col_idx] = re.sub(rgxpattern, replacement, line[col_idx])
    for country in country_list:
        if country in line[col_idx]: return None
    return line

def PostalSplit(line, std_file, col_idx=6):
    regex_address_postal = '(\s|-|,|\.|\()\d{5}($|\s|,|\)|\.|-)'; regex_postal = '\d{5}'
    postal_find = re.findall(regex_address_postal, line[col_idx])
    postal_code = None
    if len(postal_find)==1:
        postal_code=re.findall(regex_postal, postal_find[0])[0]
    if postal_code:
        for region in std_file:
            postal_range=region['postal_range'].strip().split('-')
            if int(postal_code)>=int(postal_range[0]) and int(postal_code)<=int(postal_range[1]): 
                line.append(region['city'])
                line.append(region['province'])
                return line, None
        return None, line
    else:
        return None, line

def PatternSplit(rec_list, std_file, col_idx=6):
    pass

def GenerateFileName(bucket_base, dimension, year, extension, file_id=None, temp_folder=None):    
    _temp_folder = temp_folder+'/' if temp_folder else ''
    if file_id:
        zero_prefix= '00' if file_id<10 else '0' if file_id <100 else ''
        _file_id = zero_prefix+str(file_id)
        return dimension+'/'+str(year)+'/'+bucket_base+'_'+dimension+'_'+str(year)+'_'+_file_id+'.'+extension       
    else:
        return _temp_folder+dimension+'/'+bucket_base+'_'+dimension+'_'+str(year)+'.'+extension

def CreateCSVLine(fields, delimiter="\t", lineterminator='\n'):
    line = ""
    for i in range(len(fields)):
        if i<len(fields)-1:
            line+=str(fields[i])+delimiter
        else:
            line+=str(fields[i])+lineterminator
    return line

def BytesToDataFrame(databytes, fields, delimiter="\t", lineterminator='\n'):
    try:
        output = StringIO()
        output.write(CreateCSVLine(fields))
        output.write(databytes.decode('utf-8'))
        output.seek(0)
        df = pd.read_csv(output,delimiter=delimiter,lineterminator=lineterminator)
        return df
    except:
        return None

def BytesToLines(databytes, delimiter='\t', line_list=False):
    in_file = StringIO(databytes.decode('utf-8'))
    lines=[]
    for line in in_file:
        lines.append(line.strip().split(delimiter)) if line_list else lines.append(line)
    return lines

def LinesToDataFrame(lines, fields, delimiter="\t", lineterminator='\n'):
    output = StringIO()
    output.write(CreateCSVLine(fields))
    for i in range(len(lines)):
        output.write(CreateCSVLine(lines[i]))
        if i==0: output.seek(0)
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