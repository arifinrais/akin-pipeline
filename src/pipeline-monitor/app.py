from flask import Flask, render_template, request, jsonify
import config, json
from datetime import datetime
from rejson import Client, Path

app = Flask(__name__)

def load_configs():
    configs=[item for item in dir(config) if not item.startswith("__")]
    settings={}
    for item in configs:
        settings[item]=getattr(config, item)
    all_locks=[settings['LOCK_INGEST'],settings['LOCK_AGGREGATE'],settings['LOCK_ANALYZE']]
    redis_conn = Client(host=settings['JOB_REDIS_HOST'], 
                    port=settings['JOB_REDIS_PORT'], 
                    password=settings['JOB_REDIS_PASSWORD'],
                    db=settings['JOB_REDIS_DB'],
                    decode_responses=True,
                    socket_timeout=settings['JOB_REDIS_SOCKET_TIMEOUT'],
                    socket_connect_timeout=settings['JOB_REDIS_SOCKET_TIMEOUT'])
    return settings, all_locks, redis_conn
settings, all_locks, redis_conn = load_configs()
def _read_job_stats():
    job_stats={}
    job_stats['timestamp']=datetime.utcnow().isoformat()
    job_stats['patent']=[];job_stats['trademark']=[];job_stats['publication']=[]
    for _key in redis_conn.scan_iter(match='[pt][rtu][bdn]_[0-9][0-9][0-9][0-9]',count=100): #match="[pt][rtu][bdn]_[0-9][0-9][0-9][0-9]"
        if _key in all_locks: continue
        try: #ini bingung kadang kalau gapake json.loads gakebaca, tapi kalau gaada kadang TypeError
            _job=json.loads(redis_conn.jsonget(_key, Path('.')))
        except TypeError:
            _job=redis_conn.jsonget(_key, Path('.'))
        job_stat = {}
        job_stat['year']=_job['year']
        job_stat['job']=_job['job']
        job_stat['status']=_job['status']
        job_stat['timestamp']=_job['timestamp']
        job_stat['errormsg']=_job['errormsg']
        if _job['dimension']==settings['DIMENSION_PATENT']: job_stats['patent'].append(job_stat)
        if _job['dimension']==settings['DIMENSION_PUBLICATION']: job_stats['publication'].append(job_stat)
        if _job['dimension']==settings['DIMENSION_TRADEMARK']: job_stats['trademark'].append(job_stat)
    return job_stats

@app.route("/", methods=["GET", "POST"])
def home():
    #print(request.form)
    #print(request.form.get("account"))
    return render_template("form.html")

@app.route("/get_values", methods=["GET"])
def getVal():
    #print(request.form)
    #print(request.form.get("account"))
    temp = _read_job_stats()
    print(temp)
    return jsonify(temp)