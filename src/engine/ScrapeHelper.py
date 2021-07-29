#!/usr/bin/env python3
import json
import requests as req
from io import BytesIO
from minio import Minio

def Scrape(req_item, dimension, year, minio_settings, file_id):
    BUCKET_NAME='raw'
    MINIO_CLIENT = Minio(
        minio_settings['MINIO_HOST']+':'+str(minio_settings['MINIO_PORT']),
        access_key=minio_settings['MINIO_ROOT_USER'],
        secret_key=minio_settings['MINIO_ROOT_PASSWORD'],
        secure=False
    )
    FILE_NAME=dimension+'/'+str(year)+'/'+BUCKET_NAME+'_'+dimension+'_'+str(year)
    if file_id:
        _file_id='00' if file_id<10 else '0' if file_id<100 else ''
        _file_id=_file_id+str(_file_id)
        FILE_NAME=FILE_NAME+'_'+_file_id
    resp=req.get(req_item['url'])
    num_of_records = 0
    if dimension == 'ptn' or dimension=='trd':
        FILE_NAME=FILE_NAME+'.json'
        resp_dict = resp.json()
        num_of_records = resp_dict['hits']['total']['value']
        if not num_of_records: return #if there are no records, don't do anything
        content = json.dumps(resp_dict['hits'], ensure_ascii=False, indent=4).encode('utf-8') # convert dict to bytes
        _content_type='application/json'
    elif dimension=='pub':
        FILE_NAME=FILE_NAME+'.html'
        #need some handling for pages in html
        if not num_of_records: return
        content=resp.text.encode('utf-8') #convert text/html to bytes for reverse conversion use bytes.decode()
        _content_type='text/html'
    result = MINIO_CLIENT.put_object(BUCKET_NAME, FILE_NAME, BytesIO(content), length=-1, part_size=5*1024*1024, content_type=_content_type) #assuming maximum json filesize 1MB, minimum 5MiB
    print(result.object_name)
    return result.object_name
