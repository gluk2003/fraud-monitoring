"""End-to-end checks against actual Kafka, PostgreSQL, Grafana and Airflow.

Run on a clean stack: python3 scripts/verify_stack.py [--alerts]
The alert scenario temporarily changes input and stops the simulator, then restores it.
"""
import argparse
import base64
import json
import math
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
REPORT = {'checks': []}


def request(path, data=None, grafana=False):
    headers = {}
    if grafana:
        headers['Authorization'] = 'Basic ' + base64.b64encode(b'admin:admin').decode()
    if data is not None:
        headers['Content-Type'] = 'application/json'
    url = ('http://localhost:3000' if grafana else 'http://localhost:9090') + path
    req = urllib.request.Request(url, data=json.dumps(data).encode() if data is not None else None, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def compose(*args, timeout=300):
    result = subprocess.run(['docker', 'compose', *args], cwd=BASE, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stdout[-12000:])
    return result.stdout.strip()


def wait_for(description, fn, timeout=180):
    end = time.monotonic() + timeout
    error = None
    while time.monotonic() < end:
        try:
            result = fn()
            if result:
                print('PASS', description, flush=True)
                REPORT['checks'].append(description)
                return result
        except Exception as e:
            error = e
        time.sleep(2)
    raise AssertionError(f'Timeout: {description}; last error={error}')


def prom(expr):
    result = request('/api/v1/query?' + urllib.parse.urlencode({'query': expr}))
    assert result['status'] == 'success', result
    return result['data']['result']


def scalar(expr):
    result = prom(expr)
    return float(result[0]['value'][1]) if result else 0


def pg(query):
    result = request('/api/ds/query', {
        'from': str(int((time.time() - 900) * 1000)), 'to': str(int(time.time() * 1000)),
        'queries': [{'refId': 'A', 'datasource': {'type': 'grafana-postgresql-datasource', 'uid': 'postgres'},
                     'rawSql': query, 'format': 'table', 'rawQuery': True, 'intervalMs': 5000, 'maxDataPoints': 1000}],
    }, grafana=True)['results']['A']
    assert not result.get('error'), result
    assert result.get('frames'), result
    return result['frames']


def values(frames):
    return frames[0]['data']['values']


def sqlquote(s):
    return "'" + s.replace("'", "''") + "'"


def dashboards_have_data():
    for path in sorted((BASE / 'grafana/dashboards').glob('*.json')):
        d = json.loads(path.read_text())
        saved = request('/api/dashboards/uid/' + d['uid'], grafana=True)
        assert saved['dashboard']['title'] == d['title']
        for p in d['panels']:
            for target in p['targets']:
                if 'expr' in target:
                    expr = target['expr'].replace('$__rate_interval', '15s')
                    rows = prom(expr)
                    assert rows, (p['title'], expr)
                    assert any(math.isfinite(float(r['value'][1])) for r in rows), p['title']
                else:
                    query = target['rawSql'].replace('${state:sqlstring}', "'__all'").replace('${merchant:sqlstring}', "'__all'")
                    assert values(pg(query))[0], p['title']
    return True


def check_sql_filters():
    # Includes merchant apostrophes to exercise SQL escaping used by :sqlstring.
    sample = values(pg("SELECT us_state, merch, count(*)::int AS n FROM scores WHERE merch LIKE '%''%' GROUP BY us_state,merch ORDER BY n DESC LIMIT 1"))
    state, merchant = sample[0][0], sample[1][0]
    d = json.loads((BASE / 'grafana/dashboards/fraud-business.json').read_text())
    for name, query in [('state', d['templating']['list'][0]['query']),
                        ('merchant', d['templating']['list'][1]['query'])]:
        assert values(pg(query.replace('${state:sqlstring}', sqlquote(state))))[0], name
    for p in d['panels']:
        query = p['targets'][0]['rawSql'].replace('${state:sqlstring}', sqlquote(state)).replace('${merchant:sqlstring}', sqlquote(merchant))
        frames = pg(query)
        if 'Плотность' in p['title']:
            assert abs(sum(values(frames)[1]) * .05 - 1) < 1e-8
        if 'TPS' in p['title']:
            assert any(x > 0 for x in values(frames)[1])
    # Empty selection must return a zero density, without division by zero.
    query = d['panels'][0]['targets'][0]['rawSql'].replace('${state:sqlstring}', "'NO_SUCH_STATE'").replace('${merchant:sqlstring}', "'__all'")
    assert sum(values(pg(query))[1]) == 0
    REPORT['segment'] = {'state': state, 'merchant': merchant}
    return True


def firing(uid):
    groups = request('/api/prometheus/grafana/api/v1/rules', grafana=True)['data']['groups']
    return any(r.get('state') == 'firing' and r.get('name') == uid for g in groups for r in g['rules'])


def check_alerts():
    try:
        compose('exec', '-T', 'scorer', 'python', '-m', 'services.demo', 'high-fraud')
        wait_for('Grafana high fraud alert fires', lambda: firing('Fraud ratio above 20%'), timeout=150)
        compose('stop', 'simulator')
        wait_for('Grafana stopped stream alert fires', lambda: firing('No successful scoring for 20 seconds'), timeout=120)
    finally:
        compose('exec', '-T', 'scorer', 'python', '-m', 'services.demo', 'normal')
        compose('start', 'simulator')
    wait_for('Grafana alerts recover', lambda: not firing('Fraud ratio above 20%') and not firing('No successful scoring for 20 seconds'), timeout=150)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--alerts', action='store_true')
    args = parser.parse_args()
    wait_for('Prometheus has three live targets', lambda: scalar('sum(up{job=~"scorer|writer|node"})') == 3)
    wait_for('Grafana healthy', lambda: request('/api/health', grafana=True).get('database') == 'ok')
    initial = scalar('fraud_scored_total')
    wait_for('Live Kafka input grows scoring and writer counters', lambda: scalar('fraud_scored_total') > initial + 20 and scalar('fraud_db_written_total') > 20)
    wait_for('All four dashboards and every panel have live data', dashboards_have_data)
    wait_for('PostgreSQL state/merchant filters and normalized density work', check_sql_filters)
    rules = request('/api/v1/provisioning/alert-rules', grafana=True)
    assert {r['uid'] for r in rules} == {'fraud-high-ratio', 'fraud-stopped-stream'}
    REPORT['checks'].append('Two Grafana alert rules provisioned')
    container = compose('ps', '-q', 'scorer')
    old = values(pg('SELECT model_version FROM scores ORDER BY scored_at DESC LIMIT 1'))[0][0]
    assert old == 'bootstrap', 'Use a clean stack for deterministic first retraining acceptance'
    # Supply and DQ remain functional from HW3.
    logical_date = datetime.now(timezone.utc).isoformat()
    for dag in ['supply_labeled_month', 'dataset_quality', 'retrain_fraud_model']:
        output = compose('exec', '-T', 'airflow-scheduler', 'airflow', 'dags', 'test', dag, logical_date)
        assert 'state=success' in output or 'state: success' in output, output[-4000:]
        print('PASS Airflow DAG', dag, flush=True)
        REPORT['checks'].append('Airflow DAG ' + dag)
    version_rows = wait_for('Hot reload counter appears', lambda: prom('fraud_model_reloads_total > 0'))
    new_version = version_rows[0]['metric']['version']
    assert new_version != old
    wait_for('New model annotation is visible for 30s', lambda: prom('fraud_model_reloads_total > 0 and on(version) (time() - fraud_model_reload_timestamp_seconds < 30)'))
    wait_for('PostgreSQL receives new model version', lambda: values(pg('SELECT count(*) FROM scores WHERE model_version=' + sqlquote(new_version)))[0][0] > 0)
    assert compose('ps', '-q', 'scorer') == container
    restart_count = subprocess.check_output(['docker', 'inspect', '-f', '{{.RestartCount}}', container], text=True).strip()
    assert restart_count == '0', restart_count
    REPORT['model_versions'] = [old, new_version]
    # Real DB duplicate insertion, checked using writer's production function.
    code = "import psycopg2; from services.writer import DSN,insert_score; from psycopg2.extras import RealDictCursor; db=psycopg2.connect(DSN); c=db.cursor(cursor_factory=RealDictCursor); c.execute('SELECT * FROM scores LIMIT 1'); row=dict(c.fetchone()); assert insert_score(db,row) is False; print('deduplicated')"
    assert 'deduplicated' in compose('exec', '-T', 'writer', 'python', '-c', code)
    REPORT['checks'].append('PostgreSQL primary key ignores duplicate scores')
    if args.alerts:
        check_alerts()
    REPORT['finished_at'] = datetime.now(timezone.utc).isoformat()


if __name__ == '__main__':
    try:
        main()
        REPORT['success'] = True
    finally:
        (BASE / 'artifacts').mkdir(exist_ok=True)
        (BASE / 'artifacts/verification.json').write_text(json.dumps(REPORT, ensure_ascii=False, indent=2) + '\n')
