from flask import Flask, render_template, request, jsonify
from jsonschema.validators import validate
from jsonschema import ValidationError
import config, json, os
from datetime import datetime
from rejson import Client, Path

app = Flask(__name__)

def load_configs():
    configs=[item for item in dir(config) if not item.startswith("__")]
    settings={}
    for item in configs:
        settings[item]=getattr(config, item)
    all_locks=[settings['LOCK_INGEST'],settings['LOCK_AGGREGATE'],settings['LOCK_ANALYZE']]

    redis_conn = Client(host=os.getenv('JOB_REDIS_HOST'), 
                    port=os.getenv('JOB_REDIS_PORT'), 
                    password=os.getenv('JOB_REDIS_PASSWORD'),
                    db=os.getenv('JOB_REDIS_DB'),
                    decode_responses=True,
                    socket_timeout=int(os.getenv('JOB_REDIS_SOCKET_TIMEOUT')),
                    socket_connect_timeout=int(os.getenv('JOB_REDIS_SOCKET_TIMEOUT')))
    feed_schema = {
                "type": "object",
                "properties": {
                    "dimension": {"type": "string", "pattern": "^ptn|pub|trd|patent|publication|trademark$"},
                    "year": {"type": "integer", "minimum": settings['MIN_SCRAPE_YEAR'], "maximum": settings['MAX_SCRAPE_YEAR']}
                }
            }
    return settings, all_locks, redis_conn, feed_schema
settings, all_locks, redis_conn, feed_schema = load_configs()
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
    return render_template("index.html")

@app.route("/get_status", methods=["GET"])
def getVal():
    #print(request.form)
    #print(request.form.get("account"))
    temp = _read_job_stats()
    print(temp)
    return jsonify(temp)

def _feed(feed_obj):
    _object = {}
    _object['dimension'] = feed_obj['dimension']
    _object['year'] = feed_obj['year']
    _object['job'] = settings['JOB_INGEST'] if not _is_pub_exist() else settings['JOB_AGGREGATE']
    _object['status'] = settings['STAT_WAIT']
    _object['timestamp'] = datetime.utcnow().isoformat()
    _object['errormsg'] = ''
    _key = feed_obj['dimension'] + '_' + str(feed_obj['year'])
    if not _is_key_exist(_key):
        redis_conn.jsonset(_key, Path.rootPath(), json.dumps(_object))

def _is_pub_exist():
    return True if redis_conn.keys('pub_[0-9][0-9][0-9][0-9]') else False

def _is_key_exist(_key):
    return True if redis_conn.keys(_key) else False

@app.route("/feed_job", methods=["POST"])
def feedJob():
    data = json.loads(request.get_data().decode('utf-8'))
    data = {'dimension':data['dimension'], 'year':int(data['year'])}
    try:
        validate(data, feed_schema)
        _feed(data)
        return jsonify({'status': 200, "errormsg": ""})
    except ValidationError:
        return jsonify({'status': 400, "errormsg": "Bad Request: Validation Error"})
    