# This file houses all default settings for the Kafka Monitor
# to override please use a custom localsettings.py file

# job-enqueuer redis host information
JOB_REDIS_HOST = 'localhost'
JOB_REDIS_PORT = 6379
JOB_REDIS_DB = 0
JOB_REDIS_PASSWORD = None
JOB_REDIS_SOCKET_TIMEOUT = 10

# rq redis host information
RQ_REDIS_HOST = 'localhost'
RQ_REDIS_PORT = 6389
RQ_REDIS_DB = 0
RQ_REDIS_PASSWORD = None
RQ_REDIS_SOCKET_TIMEOUT = 10

# minio host information
MINIO_HOST = 'localhost'
MINIO_PORT = 9000
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'

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

# main thread sleep time
SLEEP_TIME = 0.01
HEARTBEAT_TIMEOUT = 120