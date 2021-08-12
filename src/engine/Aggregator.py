#!/usr/bin/env python3
import sys, time, json, logging, regex as re
from engine.Engine import Engine
from engine.EngineHelper import CreateCSVLine
from io import BytesIO
from minio.error import S3Error

class Aggregator(Engine):
    def __init__(self):
        Engine.__init__(self)
        self.job = self.settings['JOB_AGGREGATE']
        self.bucket = self.settings['MINIO_BUCKET_AGGREGATED']
        self.previous_bucket = self.settings['MINIO_BUCKET_INGESTED']
    
    def _aggregate(self):
        logging.debug('Acquiring Lock for Aggregation Jobs...')
        key, dimension, year = self._redis_update_stat_before(self.job)
        logging.debug('Aggregating Records...')
        success, errormsg = self._aggregate_records(dimension, year)
        logging.debug('Updating Job Status...')
        self._redis_update_stat_after(key, self.job, success, errormsg)
    
    def _aggregate_records(self, dimension, year):
        try:
            if self._check_dimension_source('PDKI', dimension):
                parsed_lines=[]
                folder = dimension+'/'+str(year)+'/'
                obj_list = self._get_object_names(self.previous_bucket,folder)
                for obj_name in obj_list:
                    lines=self._parse_json(obj_name, dimension)
                    if lines is None: 
                        raise Exception('500: Internal Server Error')
                    elif lines:
                        for line in lines: parsed_lines.append(line)
                unique_lines=self._uniquify(parsed_lines)
                self._save_data_to_minio(unique_lines, self.bucket, dimension, year)
            elif self._check_dimension_source('SINTA', dimension):
                parsed_lines, folder = [], dimension+'/'
                file_list = self._get_object_names(self.previous_bucket,folder+'university/')
                for file_name in file_list:
                    lines=self._parse_csv(file_name, year)
                    if lines is None: 
                        raise Exception('500: Internal Server Error')
                    elif lines: 
                        for line in lines: parsed_lines.append(line)
                    else: continue
                if parsed_lines:
                    unique_lines=self._uniquify(parsed_lines)
                    self._save_data_to_minio(unique_lines, self.bucket, dimension, year, temp_folder='university', temp_prefolder=False)
                parsed_lines=[]
                file_list = self._get_object_names(self.previous_bucket,folder+'non_university/')
                for file_name in file_list:
                    lines=self._parse_csv(file_name, year)
                    if lines is None: 
                        raise Exception('500: Internal Server Error')
                    elif lines:
                        for line in lines: parsed_lines.append(line)
                    else: continue
                if parsed_lines:
                    unique_lines=self._uniquify(parsed_lines)
                    self._save_data_to_minio(unique_lines, self.bucket, dimension, year, temp_folder='non_university', temp_prefolder=False)
            else:
                raise Exception('405: Parser Not Found')
            return True, None
        except:
            errormsg, b, c = sys.exc_info()
            return False, errormsg

    def _get_object_names(self, bucket_name, folder):
        object_names_fetched=False
        while not object_names_fetched:
            try:
                object_names=[]
                resp = self.minio_client.list_objects(bucket_name, prefix=folder, recursive=True)
                if resp: 
                    for obj in resp:
                        object_names.append(obj.object_name)
                    object_names_fetched=True
            except:
                time.sleep(2)
        return object_names
    
    def _parse_csv(self, file_name, year):
        try:
            line_list = self._fetch_and_parse(self.previous_bucket, file_name,'csv')
            parsed_fname = file_name.strip('.csv').split('_')
            if len(parsed_fname)==4:
                _dept_id, _afil_id=parsed_fname[-1], parsed_fname[-2]
            elif len(parsed_fname)==3:
                _dept_id, _afil_id=None, parsed_fname[-1]
        except:
            raise Exception('500: Internal Server Error')
        finally:
            lines=[]
            for line in line_list:
                if not line: continue
                try:
                    _title, _indexer, _quartile, _citations = line
                except ValueError:
                    if len(line)<4:
                        _title, _indexer, _quartile, _citations = '-', line[-3], line[-2], line[-1]
                _indexer = re.sub(';+',';',_indexer.replace('amp;',''))
                try:
                    _idxname, _idxvol, _idxissue, _idxdate, _idxtype = [x.strip() for x in _indexer.split(';')]
                except ValueError:
                    temp = _indexer.split(';')
                    if len(temp)<5:
                        _idxname, _idxdate, _idxtype = '-', temp[-2], temp[-1]
                    else:
                        _idxname, _idxdate, _idxtype = temp[0], temp[-2], temp[-1]
                #    print(sys.exc_info())
                #except:
                #    print(sys.exc_info())
                #    sys.exit()
                _year = int(_idxdate.split('-')[0])
                if year==_year:
                    if _dept_id:
                        lines.append(CreateCSVLine([_title,_idxtype,_quartile,_citations,_idxname,_year, _afil_id, _dept_id]))
                    else:
                        lines.append(CreateCSVLine([_title,_idxtype,_quartile,_citations,_idxname,_year, _afil_id]))
            return lines

    def _parse_json(self, obj_name, dimension):
        try:
            json_obj = self._fetch_and_parse(self.previous_bucket, obj_name,'json')
        except:
            raise Exception('500: Internal Server Error')
        finally:
            lines = []
            records = json_obj['hits']
            #counter=0 #buat ngecek aja
            if dimension==self.settings['DIMENSION_PATENT']:
                for record in records:
                    id_application, id_certificate, status, date_begin, date_end = \
                        record['_source']['id'], \
                        record['_source']['nomor_sertifikat'], \
                        record['_source']['status_permohonan'], \
                        record['_source']['tanggal_dimulai_perlindungan'], \
                        record['_source']['tanggal_berakhir_perlindungan']
                    if not id_certificate: continue #filter applied but not listed patent
                    address = self._parse_address(dimension, record['_source']['owner'], record['_source']['inventor'])
                    if not address: continue #patent can't be located or it's not an indonesian patent
                    classes = self._parse_classes(dimension,record['_source']['ipc'])
                    if not classes: continue
                    lines.append(CreateCSVLine([id_application,id_certificate,status, date_begin, date_end, classes, address]))
                    #counter+=1
            elif dimension==self.settings['DIMENSION_TRADEMARK']:
                for record in records:
                    id_application, id_certificate, status, date_begin, date_end = \
                        record['_source']['id'], \
                        record['_source']['nomor_pendaftaran'], \
                        record['_source']['status_permohonan'], \
                        record['_source']['tanggal_dimulai_perlindungan'], \
                        record['_source']['tanggal_berakhir_perlindungan']
                    if not id_certificate: continue #filter applied but not listed patent
                    address = self._parse_address(dimension, record['_source']['owner'])
                    if not address: continue #patent can't be located or it's not an indonesian patent
                    classes = self._parse_classes(dimension, record['_source']['t_class'])
                    if not classes: continue
                    lines.append(CreateCSVLine([id_application,id_certificate,status, date_begin, date_end, classes, address]))
            return lines
    
    def _parse_address(self, dimension, owner_record, inventor_record=None):
        owner_address=None;inventor_address=None
        for owner in owner_record:
            if dimension==self.settings['DIMENSION_PATENT']:
                if owner['nationality']=='ID' or (owner['country'] and owner['country']['code']=='ID'):
                    #bisa dicek alamatnya di Indo juga?
                    owner_address=owner['alamat_pemegang'] if owner['alamat_pemegang'] != '-' else None 
                    if not owner_address: break
            elif dimension==self.settings['DIMENSION_TRADEMARK']:
                if owner['country_code']=='ID':
                    owner_address=owner['tm_owner_address'] if owner['tm_owner_address'] != '-' else None 
                    if not owner_address: break
        if dimension==self.settings['DIMENSION_PATENT'] and inventor_record:
            for inventor in inventor_record:
                if inventor['nationality']=='ID':
                    #bisa dicek alamatnya di Indo juga?
                    inventor_address=inventor['alamat_inventor'] if inventor['alamat_inventor'] != '-' else None
                    if not inventor_address: break
        address = inventor_address if inventor_address else owner_address
        if address:
            #tar coba dicek kalau \n \r \t aja
            address = address.replace('\n',', ')
            address = address.replace('\r',', ')
            address = address.replace('\t',', ')
        return address

    def _parse_classes(self, dimension, class_record):
        if not class_record or len(class_record)==0: 
            return None #handle unclassified patent
        if dimension==self.settings['DIMENSION_PATENT']:
            record_list=[i['ipc_full'] for i in class_record]
            if len(record_list)==1 and ';' in record_list[0]: 
                record_list=record_list[0].strip().split(';') #handle error in classes
            class_list=[]
            for ipc in record_list:
                category = ipc.strip().split()
                if category[0][:-1] not in class_list:
                    class_list.append(category[0][:-1]) #ambil tiga digit awal
        elif dimension==self.settings['DIMENSION_TRADEMARK']:
            class_list=[i['class_no'] for i in class_record]
            if len(class_list)==1 and ';' in class_list[0]: 
                class_list=class_list[0].strip().split(';') #handle error in classes
        classes=''
        for _class in class_list: classes=classes+_class+';'
        return classes[:-1]

    def _uniquify(self, lines):
        unique_lines=[]
        seen = set()
        for line in lines:
            if line in seen: continue
            seen.add(line)
            unique_lines.append(line)
        return unique_lines

    def start(self):
        logging.info("Starting Aggregator Engine...")
        setup_redis = self._setup_redis_conn()
        setup_minio = self._setup_minio_client(self.bucket)
        logging.info("Aggregator Engine Successfully Started") if  setup_redis and setup_minio else logging.warning("Problem in Starting Aggregator Engine")
        while True:
            self._aggregate()
            time.sleep(self.settings['SLEEP_TIME'])
