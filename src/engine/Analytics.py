#!/usr/bin/env python3
import sys, time, pandas as pd
from engine.Engine import Engine
from engine.EngineHelper import GenerateFileName, BytesToLines
from datetime import datetime
from io import BytesIO, StringIO
from pymongo import MongoClient

class Analytics(Engine):
    COLUMNS=['id_base_class','id_detail_class','id_city','id_province','id_island', 'id_dev_econ', 'id_dev_main', 'weight']   

    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_ANALYZE']
        self.previous_bucket = self.settings['MINIO_BUCKET_TRANSFORMED']
        self.resources_bucket = self.settings['MINIO_BUCKET_RESOURCES']
        self.result_folder = self.settings['MINIO_RESULT_FOLDER']
        self.encoder_dictionary = self.settings['RES_FILES']['ANL_ENC_STD']
        self.collections = self.settings['MONGO_COLLECTIONS'] #viz, anl

    def _analyze(self):
        #key, dimension, year = self._redis_update_stat_before(self.job)
        #success, errormsg = self._analyze_file(dimension, year)
        #self._redis_update_stat_after(key, self.job, success, errormsg)
        success, errormsg = self._analyze_file('ptn', 2018)
        print(success, errormsg)

    
    def _analyze_file(self, dimension, year):
        file_name=GenerateFileName(self.previous_bucket, dimension, year, 'csv', temp_folder=self.result_folder)
        try:
            line_list = self._fetch_and_parse(self.previous_bucket, file_name, 'csv')

            df_encoded = self._encode(line_list, dimension, self.COLUMNS)
            #_class_base, _class_detail, weight, _city, _province, _island, _dev_main, _dev_econ
            df_sums = self._summarize(df_encoded)

            viz_schemes = self._translate_viz(df_sums, dimension, year)
            self._save_to_mongodb(viz_schemes, dimension, year)

            anl_schemes = self._complexity_analysis(df_sums, dimension, year)
            self._save_to_mongodb(anl_schemes, dimension, year)
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    def _encode(self, line_list, dimension, columns, class_delimiter=';', decimal_places=2):
        std_file = self._fetch_and_parse(self.resources_bucket, self.encoder_dictionary, 'json')
        ll_encoded = []
        for line in line_list:
            classes, city, province = line[5], line[7], line[8]
            _city, _province, _island, _dev_main, _dev_econ = self._encode_region(city, province, std_file)
            _classes = classes.split(class_delimiter)
            if _island!=-1 and _dev_main!=-1:
                weight = round((1/len(_classes)), decimal_places)
                for _class in _classes:
                    _class_base, _class_detail = self._encode_class(_class, std_file, dimension)
                    if _class_base!=-1 and  _class_detail!=-1:
                        ll_encoded.append([_class_base, _class_detail, _city, _province, _island, _dev_econ, _dev_main, weight])
        df_encoded = pd.DataFrame(ll_encoded,columns=columns)
        return df_encoded
    
    def _encode_region(self, city, province, std_file):
        _city, _province, _island, _dev_main, _dev_econ = -1, -1, -1, -1, -1
        for rec in std_file['city']:
            if city==rec['city']:
                _city=rec['id']
                _province=rec['parent_id']
                break
        for rec in std_file['province']:
            if _province==rec['id'] and province==rec['province']:
                _island=rec['island_id']
                _dev_econ=rec['parent_id']
                break
        for rec in std_file['dev_econ']:
            if _dev_econ==rec['id']:
                _dev_main=rec['parent_id']
                break
        return _city, _province, _island, _dev_main, _dev_econ

    def _encode_class(self, _class, std_file, dimension):
        _class_base, _class_detail = -1, -1
        if dimension==self.settings['DIMENSION_PATENT']:
            for rec in std_file['ipc_base']:
                if _class[0]==rec['class']:
                    _class_base=rec['id']
                    break
            for rec in std_file['ipc_class2']:
                if _class==rec['class']:
                    _class_detail=rec['id']
                    break
        elif dimension==self.settings['DIMENSION_TRADEMARK']:
            for rec in std_file['ncl_class1']:
                if _class==rec['class']:
                    _class_detail=rec['id']
                    _class_base=rec['parent_id']
                    break
        elif dimension==self.settings['DIMENSION_PUBLICATION']:
            None #tar define buat SINTA
        return _class_base, _class_detail

    def _summarize(self, dataframe):
        df_sums={}
        df_sums['national'] = dataframe.drop(['id_detail_class','id_city','id_province','id_island', 'id_dev_main', 'id_dev_econ'], axis=1)\
            .groupby('id_base_class').sum().reset_index()
        df_sums['province'] = dataframe.drop(['id_island','id_dev_main','id_dev_econ','id_city'], axis=1)\
            .groupby(['id_province','id_base_class','id_detail_class']).sum().reset_index()
        df_sums['city'] = dataframe.drop(['id_island','id_dev_main','id_dev_econ','id_province'], axis=1)\
            .groupby(['id_city','id_base_class','id_detail_class']).sum().reset_index()
        df_sums['dev_main'] = dataframe.drop(['id_island','id_dev_econ','id_province','id_city'], axis=1)\
            .groupby(['id_dev_main','id_base_class','id_detail_class']).sum().reset_index()
        df_sums['dev_econ'] = dataframe.drop(['id_island','id_dev_main','id_province','id_city'], axis=1)\
            .groupby(['id_dev_econ','id_base_class','id_detail_class']).sum().reset_index()
        df_sums['island'] = dataframe.drop(['id_dev_main','id_dev_econ','id_province','id_city'], axis=1)\
            .groupby(['id_island','id_base_class','id_detail_class']).sum().reset_index()
        return df_sums

    def _translate_viz(self, dataframes, dimension, year):
        if dimension==self.settings['DIMENSION_PATENT']:
            None
        elif dimension==self.settings['DIMENSION_TRADEMARK']:
            None
        elif dimension==self.settings['DIMENSION_PUBLICATION']:
            None
        #_class_base, _class_detail, weight, _city, _province, _island, _dev_main, _dev_econ
        #translate
        #save to mongodb
        pass

    def _decode(self, code, enc_type, enc_dimension):
        std_file = self._fetch_and_parse(self.resources_bucket, self.encoder_dictionary, 'json')
        if enc_type not in ['region','class']: raise Exception('403: Encoder Type Not Recognized')
        for rec in std_file[enc_dimension]:
            if rec['id']==code:
                if enc_type=='region': return rec[enc_dimension]
                if enc_type=='class': return rec['class']
        raise Exception('403: Code Not Recognized')

    def _translate_anl(line_list, dimension, year):
        pass

    def _complexity_analysis(self, resp, dimension, year):
        pass

    def _save_to_mongodb(self, resp, analyses, year):
        complexity_collection = self.mongo_database[self.settings['MONGODB_COLLECTION_REGIONAL_PATENT']]
        None

    def start(self):
        self._setup_redis_conn()
        self._setup_minio_client()
        self._setup_mongo_client()
        while True:
            self._analyze()
            time.sleep(self.settings['SLEEP_TIME'])

