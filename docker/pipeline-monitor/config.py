# This file houses all default settings for the Kafka Monitor
# to override please use a custom localsettings.py file

# Job-Mntr redis host information
JOB_REDIS_HOST = 'job-enqueuer'
JOB_REDIS_PORT = 6379
JOB_REDIS_DB = 0
JOB_REDIS_PASSWORD = None
JOB_REDIS_SOCKET_TIMEOUT = 10

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