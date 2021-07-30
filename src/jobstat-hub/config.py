#!/usr/bin/env python3

# job-enqueuer redis host information
JOB_REDIS_HOST = 'localhost'
JOB_REDIS_PORT = 6379
JOB_REDIS_DB = 0
JOB_REDIS_PASSWORD = None
JOB_REDIS_SOCKET_TIMEOUT = 10

# ingestion settings
MAX_INGEST_YEAR = 2018
MIN_INGEST_YEAR = 2000

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