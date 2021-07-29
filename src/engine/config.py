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
MINIO_HOST = 'minio' #minio for docker
MINIO_PORT = 9000
MINIO_ROOT_USER = 'minio'
MINIO_ROOT_PASSWORD = 'minio123'
MINIO_BUCKET_INGESTED = 'raw'
MINIO_BUCKET_AGGREGATED = 'agg'
MINIO_BUCKET_TRANSFORMED = 'tfm'

# ingestion settings
MAX_INGEST_YEAR = 2018
MIN_INGEST_YEAR = 2000

# spark settings
SPARK_MASTER = 'spark://172.17.0.6:7077'
SPARK_APP_NAME = 'preparator'
SPARK_SUBMIT_DEPLOY_MODE = 'client'
SPARK_UI_SHOW_CONSOLE_PROGRESS = 'true'
SPARK_EVENT_LOG_ENABLED = 'false'
SAPRK_LOG_CONF_ = 'false'
SPARK_DRIVER_BIND_ADDRESS = '0.0.0.0'
SPARK_DRIVER_HOST = '172.25.0.5'

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
HEARTBEAT_TIMEOUT = 120