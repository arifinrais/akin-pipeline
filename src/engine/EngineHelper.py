#!/usr/bin/env python3
import json, sys, json, traceback, requests as req, pandas as pd, regex as re
from numpy import add #, time, csv, os, logging
from urllib.parse import quote
#import requests as req
from minio import Minio
from engine import config
from datetime import datetime
from rejson import Client, Path
from minio import Minio
from io import BytesIO, StringIO
from fuzzywuzzy import fuzz
from bs4 import BeautifulSoup
from math import ceil

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
        num_of_records = 0
        if dimension == 'ptn' or dimension=='trd':
            FILE_NAME=dimension+'/'+str(year)+'/'+BUCKET_NAME+'_'+dimension+'_'+str(year)
            if file_id:
                zero_prefix='00' if file_id<10 else '0' if file_id<100 else ''
                FILE_NAME+='_'+zero_prefix+str(file_id)
            FILE_NAME+='.json'
            resp_dict=None
            with req.get(req_item['url']) as resp:
                resp_dict = resp.json()
            num_of_records = resp_dict['hits']['total']['value']
            content = json.dumps(resp_dict['hits'], ensure_ascii=False, indent=4).encode('utf-8') # convert dict to bytes
            _content_type='application/json'
        elif dimension=='pub':
            _type=req_item['afil_type']
            FILE_NAME=dimension+'/'
            _folder = 'university/' if _type=='uni' else 'non_university/'
            _dept_id = '_'+req_item['params']['id'] if _type=='uni' else ''
            _afil_id = req_item['params']['afil'] if _type=='uni' else req_item['params']['id']
            FILE_NAME+=_folder+BUCKET_NAME+'_'+dimension+'_'+_afil_id+_dept_id+'.csv'
            _lines=[]
            pages, num_of_records= GetPages(req_item)
            for i in range(pages):
                new_params=req_item['params']
                new_params['page']=str(i+1)
                with req.get(req_item['url'], params=new_params) as resp:
                    soup = BeautifulSoup(resp.text, features="html.parser") 
                    _titles, _quartiles, _citations, _indexers=[],[],[],[]
                    for record in soup.findAll("a",{"class":"paper-link"}):
                        _titles.append(str(record).split('>')[1].split('<')[0])
                    soups = soup.findAll("td",{"class":"index-val uk-text-center"})
                    for i in range(0,len(soups),2):
                        _quartiles.append(str(soups[i]).split('>')[1].split('<')[0])
                        _citations.append(str(soups[i+1]).split('>')[1].split('<')[0])
                    for record in soup.findAll("dd",{"class":"indexed-by"}):
                        temp = str(record).split('>')[1].split('<')[0].strip()\
                            .replace(' |',';').replace('\t',', ').replace('\n',', ').replace('\r',', ')
                        _indexers.append(re.sub('\s+',' ',temp))
                    for i in range(len(_titles)):
                        _lines.append(CreateCSVLine([_titles[i],_indexers[i],_quartiles[i],_citations[i]]))
            csv_file = StringIO(newline='\n')
            for line in _lines: csv_file.writelines(line)
            content = csv_file.getvalue().encode('utf-8')
            _content_type='application/csv'
        if num_of_records:
            result = MINIO_CLIENT.put_object(BUCKET_NAME, FILE_NAME, BytesIO(content), length=-1, part_size=5*1024*1024, content_type=_content_type) #assuming maximum json filesize 1MB, minimum 5MiB
            return result.object_name
        else:
            return '404: 0 number of records'
    except:
        emssg, b, c =sys.exc_info()
        return emssg

def GetPages(req_item):
    pages,num_of_records=0,0
    with req.get(req_item['url'], params=req_item['params']) as resp:
        soup = BeautifulSoup(resp.text, features="html.parser")
        for record in soup.findAll("caption"):
            num_of_records=int(str(record).split(':')[1].split('<')[0].strip())
            pages = ceil(num_of_records/10)
    return pages, num_of_records

def CleanAddress(line, regexp_list, country_list, col_idx=6):
    for regex_pair in regexp_list:
        rgxpattern, replacement = regex_pair
        line[col_idx] = re.sub(rgxpattern, replacement, line[col_idx])
    for country in country_list:
        if country.lower() in line[col_idx].lower(): return None
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
            city1, city2 = region['city'].lower(), region['city_official'].lower()
            rating1, rating2 = fuzz_calc(ps_city, city1),fuzz_calc(ps_city, city2)
            rating3 = fuzz_calc(ps_city, region['city_alias'].lower()) if region['city_alias'] else 0
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

def InstitutionMapping(line_list, std_file, columns, map_col):
    _afil_type = 'uni' if len(columns)==8 else 'non_uni' if len(columns)==7 else None
    _used, _dropped = ('university','non_university') if _afil_type=='uni' else ('non_university','university')
    df_lines = pd.DataFrame(line_list, columns=columns).sort_values(map_col, axis=0)
    df_std = pd.json_normalize(std_file).drop(_dropped, axis=1).explode(_used).reset_index(drop=True).dropna().sort_values(_used,axis=0)
    res = df_lines.join(df_std.set_index(_used), on=[map_col])
    return res.values.tolist()

def DepartmentMapping(line_list, std_file, columns, map_col):
    df_lines = pd.DataFrame(line_list, columns=columns).sort_values(map_col, axis=0)
    df_std = pd.json_normalize(std_file).explode('department').reset_index(drop=True).dropna().sort_values('department',axis=0)
    res = df_lines.join(df_std.set_index('department'), on=[map_col])
    return res.values.tolist()

def Geocode(line, std_file, std_postal, api_config=None, col_idx=6, fuzz_offset=88):
    try: 
        addresses = GetAddresses(line[col_idx].split())
    except IndexError:
        return None, line
    finally:
        resp_city, resp_province, postal_mapped = GeocodeOSM(addresses, std_postal)
        if postal_mapped:
            line.append(resp_city)
            line.append(resp_province)
            return line, None
        if not resp_city:
            if not api_config: return None, line
            #Geocode other API if needed using API config
            return None, line

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
    if max_rating>fuzz_offset:
        line.append(max_city)
        line.append(max_province)
        return line, None
    return None, line

def GeocodeOSM(addresses, std_postal):
    GEOCODE_API = "https://nominatim.openstreetmap.org/search"
    resp_city, resp_province = None, None
    for addr in addresses:
        params={"q":addr,"format":"json","country":"Indonesia"}
        with req.get(GEOCODE_API, params=params) as resp:
            records=json.loads(resp.text)
            try:
                for rec in records:
                    _addr=rec['display_name'].split(', ')
                    _lat, _lon = float(rec['lat']), float(rec['lon'])
                    if CheckBorders(_lat, _lon) or _addr[-1].lower()!='indonesia': continue
                    if len(_addr)>=3:
                        _postal=_addr[-2]
                        if re.match('\d{1,12}', _postal):
                            if re.match('\d{5}', _postal):
                                for region in std_postal:
                                    if _postal in region['postal_codes']: return region['city'], region['province'], True
                            resp_city, resp_province = _addr[-4], _addr[-3]
                        else:
                            resp_city, resp_province = _addr[-3], _addr[-2]
                        if rec['type'] in ['administrative', 'city', 'town'] and resp_city: return resp_city, resp_province, False
            except IndexError: continue
    return resp_city, resp_province, False

def GetAddresses(addr_list, num_of_params=5):
    addresses, n_iter = [], num_of_params if len(addr_list)>=num_of_params else len(addr_list)
    for i in range(1, n_iter+1):
        addr=''
        for j in range(i):
            addr=addr_list[-j-1]+' '+addr
        addresses.append(addr.strip())
    return addresses[::-1]

def CheckBorders(lat, lon):
    NORTH_BORDER, SOUTH_BORDER, WEST_BORDER, EAST_BORDER = 6.09, -11.16, 95.46, 141.06
    return lat>NORTH_BORDER or lat<SOUTH_BORDER or lon<WEST_BORDER or lon>EAST_BORDER

def GenerateFileName(bucket_base, dimension, year, extension, file_id=None, temp_folder=None, temp_prefolder=True):    
    _temp_folder = temp_folder+'/' if temp_folder else ''
    if file_id:
        zero_prefix= '00' if file_id<10 else '0' if file_id <100 else ''
        _file_id = zero_prefix+str(file_id)
        return dimension+'/'+str(year)+'/'+bucket_base+'_'+dimension+'_'+str(year)+'_'+_file_id+'.'+extension       
    else:
        if temp_prefolder:
            return _temp_folder+dimension+'/'+bucket_base+'_'+dimension+'_'+str(year)+'.'+extension
        else:
            return dimension+'/'+_temp_folder+bucket_base+'_'+dimension+'_'+str(year)+'.'+extension

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

def LineListToLines(line_list):
    lines=[]
    for line in line_list:
        lines.append(CreateCSVLine(line))
    return lines

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