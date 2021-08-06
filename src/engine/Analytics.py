#!/usr/bin/env python3
import sys, time, pandas as pd, numpy as np
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
        self.collections = self.settings['MONGO_COLLECTIONS'] #viz, anl
        
    def _load_encoder_dictionary(self, retry=3):
        i=0 #retry mechanism bisa diganti tenacity(?)
        while True:
            self.encoder_dictionary = self._fetch_and_parse(self.resources_bucket, self.settings['RES_FILES']['ANL_ENC_STD'], 'json')
            if self.encoder_dictionary: break
            i+=1
            if i==retry: return False
            time.sleep(self.settings['SLEEP_TIME'])
        self.NUMBER_OF_CITIES = len(self.encoder_dictionary['city'])
        self.NUMBER_OF_PROVINCES = len(self.encoder_dictionary['province'])
        self.NUMBER_OF_ISLAND = len(self.encoder_dictionary['island'])
        self.NUMBER_OF_DEV_ECON = len(self.encoder_dictionary['dev_econ'])
        self.NUMBER_OF_DEV_MAIN = len(self.encoder_dictionary['dev_main'])
        self.NUMBER_OF_PATENT_CLASS = len(self.encoder_dictionary['ipc_class2'])
        self.NUMBER_OF_TRADEMARK_CLASS = len(self.encoder_dictionary['ncl_class1'])
        #self.NUMBER_OF_PUBLICATION_CLASS = len(self.encoder_dictionary['cip_class2'])
        return True

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
            #region, class_base, class_detail, weight

            viz_scheme = self._translate_viz(df_sums, dimension, year)
            self._save_to_mongodb(viz_scheme, dimension, 'viz')

            anl_scheme = self._complexity_analysis(df_sums, dimension, year)
            self._save_to_mongodb(anl_scheme, dimension, 'anl')
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg
    
    def _encode(self, line_list, dimension, columns, class_delimiter=';', decimal_places=2):
        ll_encoded = []
        for line in line_list:
            classes, city, province = line[5], line[7], line[8]
            _city, _province, _island, _dev_main, _dev_econ = self._encode_region(city, province)
            _classes = classes.split(class_delimiter)
            if _island!=-1 and _dev_main!=-1:
                weight = round((1/len(_classes)), decimal_places)
                for _class in _classes:
                    _class_base, _class_detail = self._encode_class(_class, dimension)
                    if _class_base!=-1 and  _class_detail!=-1:
                        ll_encoded.append([_class_base, _class_detail, _city, _province, _island, _dev_econ, _dev_main, weight])
        df_encoded = pd.DataFrame(ll_encoded,columns=columns)
        return df_encoded
    
    def _encode_region(self, city, province):
        _city, _province, _island, _dev_main, _dev_econ = -1, -1, -1, -1, -1
        for rec in self.encoder_dictionary['city']:
            if city==rec['city']:
                _city=rec['id']
                _province=rec['parent_id']
                break
        for rec in self.encoder_dictionary['province']:
            if _province==rec['id'] and province==rec['province']:
                _island=rec['island_id']
                _dev_econ=rec['parent_id']
                break
        for rec in self.encoder_dictionary['dev_econ']:
            if _dev_econ==rec['id']:
                _dev_main=rec['parent_id']
                break
        return _city, _province, _island, _dev_main, _dev_econ

    def _encode_class(self, _class, dimension):
        _class_base, _class_detail = -1, -1
        if dimension==self.settings['DIMENSION_PATENT']:
            for rec in self.encoder_dictionary['ipc_base']:
                if _class[0]==rec['class']:
                    _class_base=rec['id']
                    break
            for rec in self.encoder_dictionary['ipc_class2']:
                if _class==rec['class']:
                    _class_detail=rec['id']
                    break
        elif dimension==self.settings['DIMENSION_TRADEMARK']:
            for rec in self.encoder_dictionary['ncl_class1']:
                if _class==rec['class']:
                    _class_detail=rec['id']
                    _class_base=rec['parent_id']
                    break
        elif dimension==self.settings['DIMENSION_PUBLICATION']:
            None #tar define buat SINTA
        return _class_base, _class_detail

    def _decode(self, code, enc_type, enc_dimension):
        if enc_type not in ['region','class']: raise Exception('403: Encoder Type Not Recognized')
        for rec in self.encoder_dictionary[enc_dimension]:
            if rec['id']==code:
                if enc_type=='region': return rec[enc_dimension]
                if enc_type=='class': return rec['class']
        raise Exception('403: Code Not Recognized')

    def _df_to_line_list(self, dataframe):
        lines=[]
        for row in dataframe.values.tolist():
            line = [int(x) for x in row[:-1]]
            line.append(row[-1])
            lines.append(line)
        return lines

    def _summarize(self, dataframe):
        df_sums={} #note: groupby automatically sort values
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

    def _get_regional_count(self, dataframe, reg_dimension, cls_dimension):
        col_id = 'id_'+reg_dimension
        class_base, class_detail = self._get_class_classification(cls_dimension)
        counts=[]
        line_list = self._df_to_line_list(dataframe)
        if reg_dimension=='national':
            for line in line_list: counts.append({class_base: self._decode(line[0],'class',class_base),"total": line[1]})
        else:
            for line in line_list:
                _count_found=False
                for count in counts:
                    if count[col_id]==line[0]:
                        count["total"]+=line[-1]
                        _class_found=False
                        for _class in count["class"]:
                            if _class[class_base]==self._decode(line[1],'class',class_base):
                                _class["total"]+=line[-1]
                                _class["class2"].append({class_detail: self._decode(line[2],'class',class_detail), "total": line[-1]})
                                _class_found=True
                                continue
                        if not _class_found:
                            count["class"].append({class_base: self._decode(line[1],'class',class_base), "total": line[-1],
                                "class2":[{class_detail: self._decode(line[2],'class',class_detail), "total": line[-1]}]})
                        _count_found=True
                        continue
                if not _count_found:
                    counts.append({col_id: line[0], "total": line[-1], \
                        "class":[{class_base: self._decode(line[1],'class',class_base), "total": line[-1],
                            "class2":[{class_detail: self._decode(line[2],'class',class_detail), "total": line[-1]}]}]})
        return counts

    def _get_class_classification(self, dimension):
        if dimension==self.settings['DIMENSION_PATENT']: return 'ipc_base', 'ipc_class2'
        if dimension==self.settings['DIMENSION_TRADEMARK']: return 'ncl_base', 'ncl_class1'
        if dimension==self.settings['DIMENSION_PUBLICATION']: return 'cip_base', 'cip_class2'

    def _translate_viz(self, dataframes, dimension, year):
        viz_schemes={"year": year}
        viz_schemes["national"]=self._get_regional_count(dataframes["national"],"national",dimension)
        viz_schemes["province"]=self._get_regional_count(dataframes["province"],"province",dimension)
        viz_schemes["city"]=self._get_regional_count(dataframes["city"],"city",dimension)
        viz_schemes["dev_main"]=self._get_regional_count(dataframes["dev_main"],"dev_main",dimension)
        viz_schemes["dev_econ"]=self._get_regional_count(dataframes["dev_econ"],"dev_econ",dimension)
        viz_schemes["island"]=self._get_regional_count(dataframes["island"],"island",dimension)
        return viz_schemes

    def _translate_anl(self, dataframes, dimension, year):
        anl_schemes={"year": year}
        for key in dataframes:
            anl_schemes[key]['kci'], anl_schemes[key]['ipci']=self._df_to_object_list(dataframes[key], key, dimension)
        return anl_schemes

    def _df_to_object_list(self, dataframes, reg_dimension, cls_dimension):
        i=0; kci_list=[]; ipci_list=[]
        reg_id = 'id_'+reg_dimension
        region_index, class_index = dataframes['reg_idx'], dataframes['cls_idx']
        for row in dataframes['kci'].values.tolist():
            kci_list.append({reg_id: region_index[i], "value": row[0]})
            i+=1
        i=0
        for row in dataframes['ipci'].values.tolist():
            ip_base, ip_detail = self._get_class_classification(cls_dimension)
            ip_class = self._decode(class_index[i], 'class', cls_dimension)
            ipci_list.append({ip_detail: ip_class, "value": row[0]})
            i+=1
        return kci_list, ipci_list

    def _complexity_analysis(self, dataframes, dimension, year):
        results={}
        for key in dataframes:
            if key!='national':
                results[key]['kci'], results[key]['pci'], results[key]['reg_idx'], results[key]['cls_idx'] = \
                    self._complexity_index_calculation(key, dimension, dataframes[key])
        anl_schemes = self._translate_anl(results, dimension, year)
        return anl_schemes
    
    def _complexity_index_calculation(self, reg_dimenson, cls_dimension, dataframe):
        num_of_region, num_of_class = self._get_matrix_dimension(reg_dimenson,cls_dimension)
        rca_matrix = self._create_rca_matrix(dataframe, num_of_region, num_of_class) #rca_cutoff can be added
        rca_matrix, diversity_vector, ubiquity_vector, region_index, class_index = self._squeeze_rca_matrix(rca_matrix)
        kci = self._calculate_kci(rca_matrix, diversity_vector, ubiquity_vector)
        ipci = self._calculate_ipci(rca_matrix, diversity_vector, ubiquity_vector)
        return kci, ipci, region_index, class_index

    def _get_matrix_dimension(self, reg_dimension, cls_dimension):
        num_of_region = self.NUMBER_OF_CITIES if reg_dimension=='city' else self.NUMBER_OF_PROVINCES if reg_dimension=='province'\
            else self.NUMBER_OF_ISLAND if reg_dimension=='island' else self.NUMBER_OF_DEV_ECON if reg_dimension=='dev_main' else\
                self.NUMBER_OF_DEV_MAIN if reg_dimension=='dev_main' else 0
        if not num_of_region: raise Exception('403: Regional Dimension Not Recognized')
        num_of_class = self.NUMBER_OF_PATENT_CLASS if cls_dimension==self.settings['DIMENSION_PATENT'] else\
            self.NUMBER_OF_TRADEMARK_CLASS if cls_dimension==self.settings['DIMENSION_TRADEMARK'] else 0 #tar tambahin SINTA
        if not num_of_class: raise Exception('403: Class Dimension Not Recognized')
        return num_of_region, num_of_class

    def _create_rca_matrix(self, dataframe, num_of_region, num_of_class, rca_cutoff=1):
        matrix = np.zeros((num_of_region,num_of_class))
        line_list = self._df_to_line_list(dataframe.drop(['id_base_class'], axis=1))
        for line in line_list:
            matrix[line[0]][line[1]] += line[2]
        national_class_share = np.sum(matrix,axis=0)/np.round(np.sum(matrix))
        total_per_region = np.sum(matrix,axis=1)
        for i in range(num_of_region):
            if total_per_region[i]==0: continue #region not producing
            for j in range(num_of_class):
                if national_class_share[j]==0: continue #class not being produced
                matrix[i][j]/=total_per_region[i]*national_class_share[j]
        return np.where(matrix>rca_cutoff,1,0)

    def _squeeze_rca_matrix(self, rca_matrix):
        df = pd.DataFrame(rca_matrix)
        df = df.loc[:,(df != 0).any(axis=0)]
        df = df.loc[(df != 0).any(axis=1)]
        rca_matrix, region_index, class_index = df.values, df.index.values.tolist(), df.columns.values.tolist()
        diversity_vector, ubiquity_vector = np.sum(rca_matrix,axis=1), np.sum(rca_matrix,axis=0)
        return rca_matrix, diversity_vector, ubiquity_vector, region_index, class_index

    def _calculate_kci(self, rca_matrix, diversity_vector, ubiquity_vector, decimal_places=2):
        region_tilde_matrix = (rca_matrix.dot((rca_matrix/ubiquity_vector).T)/diversity_vector).T
        reg_egval, reg_egvec = np.linalg.eig(region_tilde_matrix)
        second_highest_idx = reg_egval.argsort()[-2]
        kci = np.real(reg_egvec[:,second_highest_idx])
        kci = (kci - kci.mean())/kci.std()
        kci = pd.DataFrame(kci, columns=["kci"])
        kci = self._fix_sign(diversity_vector, kci)
        return np.round(kci, decimal_places)

    def _calculate_ipci(self, rca_matrix, diversity_vector, ubiquity_vector, decimal_places=2):
        class_tilde_matrix = ((rca_matrix.T).dot((rca_matrix.T/diversity_vector).T)/ubiquity_vector).T
        cls_egval, cls_egvec = np.linalg.eig(class_tilde_matrix)
        second_highest_idx = cls_egval.argsort()[-2]
        ipci = np.real(cls_egvec[:,second_highest_idx])
        ipci = (ipci - ipci.mean())/ipci.std()
        ipci = pd.DataFrame(ipci, columns=["ipci"])
        ipci = self._fix_sign(diversity_vector, ipci)
        return np.round(ipci*-1, decimal_places)

    def _fix_sign(self, diversity_vector, complexity_index):
        corr = np.corrcoef(diversity_vector, complexity_index.iloc[:,0])[0,1]
        corr_sign = np.sign(corr)
        complexity_index *= corr_sign
        return complexity_index

    def _save_to_mongodb(self, scheme, dimension, load_for):
        collection = 'VIZ_' if load_for=='viz' else 'ANL_' if load_for=='anl' else ''
        collection += 'PATENT' if dimension==self.settings['DIMENSION_PATENT'] else 'TRADEMARK'\
            if dimension==self.settings['DIMENSION_TRADEMARK'] else 'PUBLICATION'\
            if dimension==self.settings['DIMENSION_PUBLICATION'] else ''
        if not collection: raise Exception('403: Load Purpose and/or Dimension Not Recognized')
        self.mongo_collections[collection].insert_one(scheme)

    def start(self):
        self._setup_redis_conn()
        self._setup_minio_client()
        self._setup_mongo_client()
        self._load_encoder_dictionary()
        while True:
            self._analyze()
            time.sleep(self.settings['SLEEP_TIME'])

