#!/usr/bin/env python3

# job-enqueuer redis host information
JOB_REDIS_HOST = 'localhost'
JOB_REDIS_PORT = 6379
JOB_REDIS_DB = 0
JOB_REDIS_PASSWORD = None
JOB_REDIS_SOCKET_TIMEOUT = 10

# rq redis host information
RQ_REDIS_HOST = 'localhost' #rq-server for docker koentji
RQ_REDIS_PORT = 6389
RQ_REDIS_DB = 0
RQ_REDIS_PASSWORD = None
RQ_REDIS_SOCKET_TIMEOUT = 300

# minio host information
MINIO_HOST = 'localhost' #minio for docker
MINIO_PORT = 9000
MINIO_ROOT_USER = 'minio'
MINIO_ROOT_PASSWORD = 'minio123'
MINIO_BUCKET_INGESTED = 'raw'
MINIO_BUCKET_AGGREGATED = 'agg'
MINIO_BUCKET_TRANSFORMED = 'tfm'
MINIO_BUCKET_RESOURCES = 'res'
MINIO_RESULT_FOLDER = 'tmp_mapped' #defaultnya result, tapi sebelum geocoding pake tmp_mapped dulu
RES_BASE_PATH = '/Users/arifinrais/Workspace/akin/akin-pipeline/src/engine/res/'
RES_FILES = {'ING_HIT_LST': 'pub_hit_list.txt',
            'TFM_PTM_STD': 'region_mapping.json',
            'TFM_PSM_STD': 'postal_mapping.json',
            'TFM_DPM_STD': 'departmental_mapping.json',
            'TFM_ITM_STD': 'institution_mapping.json',
            'ANL_ENC_STD': 'encoder_dictionary.json'}
            #'TFM_DPM_STD': 'departmental_mapping.json'
            #'ANL_IPC_STD': 'ipc_mapping.json'
            #'ANL_NCL_STD': 'ncl_mapping.json'

# mongodb host information
MONGO_HOST = 'localhost'
MONGO_PORT = '27017'
MONGO_USER = 'root'
MONGO_PASSWORD = 'mongo123'
MONGO_URI = 'mongodb://'+MONGO_USER+':'+MONGO_PASSWORD+'@'+MONGO_HOST+':'+MONGO_PORT+'/?authSource=admin&readPreference=primary&appname=Engine%&ssl=false'
MONGO_DATABASE = 'akin'
MONGO_COLLECTIONS = {'VIZ_PATENT':'viz_ptn','ANL_PATENT':'anl_ptn',
    'VIZ_TRADEMARK':'viz_trd','ANL_TRADEMARK':'anl_trd',
    'VIZ_PUBLICATION':'viz_pub','ANL_PUBLICATION':'anl_pub'}

# ingestion settings
MAX_INGEST_YEAR = 2018
MIN_INGEST_YEAR = 2000

# spark settings
SPARK_MASTER = 'spark://localhost:7077'
SPARK_APP_NAME = 'preparator_'
SPARK_SUBMIT_DEPLOY_MODE = 'client'
SPARK_UI_SHOW_CONSOLE_PROGRESS = 'true'
SPARK_EVENT_LOG_ENABLED = 'false'
SAPRK_LOG_CONF_ = 'false'
SPARK_DRIVER_BIND_ADDRESS = '0.0.0.0'
SPARK_DRIVER_HOST = 'preparator'

# stats setup
STATS_TOTAL = True
STATS_PLUGINS = True
STATS_CYCLE = 5
STATS_DUMP = 60
# from time variables in scutils.stats_collector class
STATS_TIMES = [
    'SECONDS_15_MINUTE',
    'SECONDS_1_HOUR',
    'SECONDS_6_HOUR',
    'SECONDS_12_HOUR',
    'SECONDS_1_DAY',
    'SECONDS_1_WEEK',
]
JOB_INGEST = 'ing'
JOB_AGGREGATE = 'agg'
JOB_TRANSFORM = 'tfm'
JOB_ANALYZE = 'anl'
LOCK_INGEST = 'ingest_lock'
LOCK_AGGREGATE = 'aggregate_lock'
LOCK_TRANSFORM = 'transform_lock'
LOCK_ANALYZE = 'analyze_lock'
STAT_WAIT = 'wait'
STAT_WIP = 'wip'
STAT_DONE = 'done'
STAT_ERROR = 'err'
DIMENSION_PATENT = 'ptn'
DIMENSION_TRADEMARK = 'trd'
DIMENSION_PUBLICATION = 'pub'

# main thread sleep time
SLEEP_TIME = 1
ENGINE_LOOP_TIME = 5 #to change sleep
HEARTBEAT_TIMEOUT = 120