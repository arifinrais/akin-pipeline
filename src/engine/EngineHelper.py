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
from fuzzywuzzy import fuzz

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
    regex_address_postal = '[\s,\-,\,,\.,\(]\d{5}(?!\d)'
    postal_find = re.findall(regex_address_postal, line[col_idx])
    postal_code = postal_find[0][1:] if postal_find else None
    if postal_code:
        for region in std_file:
            if postal_code in region['postal_codes']:
                line.append(region['city'])
                line.append(region['province'])
                return line, None
    return None, line

def PatternSplit(line, std_file, col_idx=6, fuzz_offset=88):
    def fuzz_rating(possible_city, std_file):
        max_rating, max_city, max_province = 0, None, None
        ps_city = possible_city.strip().lower()
        fuzz_calc = lambda x,y: (fuzz.ratio(x,y)+fuzz.partial_ratio(x,y)+fuzz.token_sort_ratio(x,y)+fuzz.token_set_ratio(x,y))/4
        for region in std_file:
            city1, city2, city3 = region['city'].lower(), region['city_official'].lower(), region['city_alias'].lower()
            rating1, rating2, rating3 = fuzz_calc(ps_city, city1),fuzz_calc(ps_city, city2), fuzz_calc(ps_city, city3)
            rating = rating1 if rating1>rating2 and rating1>rating3 else rating2 if rating2>rating3 else rating3
            if rating==100: return 100, region['city'], region['province']
            if rating>max_rating:
                max_rating = rating
                max_city = region['city']
                max_province = region['province']
        return max_rating, max_city, max_province
    address_params = line[col_idx].split(',')
    max_rating, max_city, max_province = 0, None, None
    num_of_params = len(address_params) 
    max_param = 3 if num_of_params>3 else 2 if num_of_params>2 else 1 if num_of_params>1 else 0
    if max_param:
        for i in range(max_param):
            reg_rating, reg_city, reg_province = fuzz_rating(address_params[-i-1],std_file)
            if reg_rating==100: 
                line.append(reg_city)
                line.append(reg_province)
                return line, None
            if reg_rating>max_rating:
                max_rating, max_city, max_province = reg_rating, reg_city, reg_province
        if max_rating>fuzz_offset:
            line.append(max_city)
            line.append(max_province)
            return line, None
    return None, line

def Geocode(line, std_file, api_config, col_idx=6):
    #hit api with api_config
    resp='some json file that has to be loaded'

    #parse response
    #if response > 1 loop parse until get a city
    resp_city='some city'
    #if theres also province attribute, get the province
    resp_province='some province'
    
    #if there's no city, failed geocode
    if not resp_city: return None, line

    fuzz_calc=lambda x,y: (fuzz.ratio(x,y)+fuzz.partial_ratio(x,y)+fuzz.token_sort_ratio(x,y)+fuzz.token_set_ratio(x,y))/4
    max_rating, max_city, max_province = 0, None, None
    for region in std_file:
        reg_city = region['city'].lower()
        reg_province = region['province'].lower()
        if resp_province:
            rating = (fuzz_calc(resp_city,reg_city)+fuzz_calc(resp_province,reg_province))/2
        if rating>max_rating:
            max_city = region['city']
            max_province = region['province']
    if max_rating>50:
        line.append(max_city)
        line.append(max_province)
        return line, None
    return None, line

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