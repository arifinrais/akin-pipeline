# This file houses all default settings for the Kafka Monitor
# to override please use a custom localsettings.py file

# Enqueuer redis host information
ENQ_REDIS_HOST = 'localhost'
ENQ_REDIS_PORT = 6379
ENQ_REDIS_DB_QUEUE = 0
ENQ_REDIS_DB_SCRAP = 1
ENQ_REDIS_DB_ERROR = 2
ENQ_REDIS_PASSWORD = None
ENQ_REDIS_SOCKET_TIMEOUT = 10

# Job-Mntr redis host information
JOB_REDIS_HOST = 'localhost'
JOB_REDIS_PORT = 6379
JOB_REDIS_DB = 0
JOB_REDIS_PASSWORD = None
JOB_REDIS_SOCKET_TIMEOUT = 10

# logging setup
LOGGER_NAME = 'kafka-monitor'
LOG_DIR = 'logs'
LOG_FILE = 'kafka_monitor.log'
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUPS = 5
LOG_STDOUT = True
LOG_JSON = False
LOG_LEVEL = 'INFO'

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
SLEEP_TIME = 0.01
HEARTBEAT_TIMEOUT = 120

# scraper context
MIN_SCRAPE_YEAR = 2000
MAX_SCRAPE_YEAR = 2018