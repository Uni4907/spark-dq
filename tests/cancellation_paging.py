"""Real HTTP/Spark cancellation, queue capacity and pagination acceptance.

Creates synthetic datasets/tasks only; cleanup cancels only IDs created here.
Run from repository root: timeout 1200 python3 tests/cancellation_paging.py
Use --verify-engine-log in Docker to prove the real Spark JVM started/exited.
"""
from e2e import BASE, LOCAL, http, upload
from pathlib import Path
from urllib.parse import urlencode
import base64
import csv
import io
import json
import subprocess
import sys
import time
import uuid

TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELLED'}
OWN_IDS = []
VERIFY_ENGINE = '--verify-engine-log' in sys.argv
CONTAINER = 'spark-dq-backend-1'


def post(path, body=None, expected=200, record=False):
    encoded = None if body is None else json.dumps(body).encode('utf-8')
    code, value = http(path, encoded, {'Content-Type': 'application/json'}, 'POST')
    assert code == expected, (path, code, value)
    if record and code == 200:
        OWN_IDS.append(value['id'])
    return value


def get(path):
    code, value = http(path)
    assert code == 200, (path, code, value)
    return value


def task(dataset, rules):
    return post('/tasks', {'datasetId': dataset['id'], 'rules': rules}, record=True)


def wait_for(task_id, wanted, seconds=310):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        current = get('/tasks/' + task_id)
        if current['status'] in wanted:
            return current
        assert current['status'] not in TERMINAL, (wanted, current)
        time.sleep(0.15)
    raise AssertionError(('Task status timeout', task_id, wanted, current))


def cancel(task_id):
    return post('/tasks/' + task_id + '/cancel')


def download(dataset_id):
    with LOCAL.open(BASE + '/datasets/' + dataset_id + '/download', timeout=30) as response:
        return response.read()


def csv_bytes(columns, rows):
    stream = io.StringIO(newline='')
    writer = csv.writer(stream, quoting=csv.QUOTE_ALL, lineterminator='\r\n')
    writer.writerow(columns)
    writer.writerows(rows)
    return stream.getvalue().encode('utf-8')


def docker_probe(script, argument):
    result = subprocess.run(['docker', 'exec', CONTAINER, 'sh', '-c', script, 'sh', argument],
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE, timeout=10, check=False)
    assert result.returncode in (0, 1), ('Read-only Docker probe failed', result.returncode,
                                       result.stderr.decode('utf-8', 'replace')[:500])
    return result.returncode == 0


def engine_alive(task_id):
    # Match only Java processes; the probe shell itself contains this argument.
    script = '''for proc in /proc/[0-9]*; do
case "$(readlink "$proc/exe" 2>/dev/null)" in */java)
  if tr '\\000' '\\n' < "$proc/cmdline" 2>/dev/null | grep -Fx -- "$1" >/dev/null; then exit 0; fi;;
esac
done
exit 1'''
    return docker_probe(script, '/data/tasks/' + task_id + '/request.json')


def prove_spark_started(task_id):
    script = 'test -f "$1" && grep -E "Running Spark version|SparkContext:" "$1" >/dev/null'
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        current = get('/tasks/' + task_id)
        assert current['status'] == 'RUNNING', current
        logged = docker_probe(script, '/data/tasks/' + task_id + '/engine.log')
        if logged and engine_alive(task_id):
            return
        time.sleep(0.25)
    raise AssertionError(('Real Spark process/log not observed', task_id))


def assert_cancelled(current, queued=False):
    assert current['status'] == 'CANCELLED', current
    assert current['finished_at'] and current['duration_ms'] is not None, current
    assert current['total_rows'] is None and current['spark_version'] is None, current
    assert all(rule['result_id'] is None and rule['metrics'] is None for rule in current['rules']), current
    if queued:
        assert current['started_at'] is None, current
    governance = current['governance']
    if governance:
        assert governance['output_dataset_id'] is None and governance['audit'] is None, current
    report = get('/tasks/' + current['id'] + '/report')
    assert report['task'] == current, report
    assert 'comparison' not in report and 'outputDataset' not in report, report
    assert 'beforeRows' not in report and 'afterRows' not in report, report
    for rule in current['rules'][:1]:
        assert http('/tasks/' + current['id'] + '/rules/' + rule['id'] + '/preview')[0] == 404
    assert cancel(current['id']) == current, 'Repeated cancel changed a terminal cancelled task'
    return report


def page(kind, **params):
    return get('/' + kind + '/page' + ('?' + urlencode(params) if params else ''))


def assert_page(value, number, size):
    assert set(('items', 'total', 'page', 'pageSize', 'totalPages')).issubset(value), value
    assert value['page'] == number and value['pageSize'] == size, value
    assert value['total'] >= 0 and value['totalPages'] == (value['total'] + size - 1) // size, value
    assert len(value['items']) <= size and len({item['id'] for item in value['items']}) == len(value['items'])
    keys = [(item['created_at'], item['id']) for item in value['items']]
    assert keys == sorted(keys, reverse=True), value


def validate_paging(prefix, datasets, expected_tasks):
    for kind in ('tasks', 'datasets'):
        defaults = page(kind)
        assert_page(defaults, 1, 20)
        first = page(kind, page=1, pageSize=3)
        second = page(kind, page=2, pageSize=3)
        assert_page(first, 1, 3)
        assert_page(second, 2, 3)
        assert first['total'] == second['total'] and first['total'] >= 4
        merged = first['items'] + second['items']
        assert len({row['id'] for row in merged}) == len(merged), (kind, merged)
        sort_keys = [(row['created_at'], row['id']) for row in merged]
        assert sort_keys == sorted(sort_keys, reverse=True), (kind, sort_keys)
        assert page(kind, page=1, pageSize=3) == first, (kind, 'Unstable first page')
        beyond = page(kind, page=first['totalPages'] + 1, pageSize=3)
        assert_page(beyond, first['totalPages'] + 1, 3)
        assert beyond['items'] == [] and beyond['total'] == first['total'], beyond
        code, legacy = http('/' + kind)
        assert code == 200 and isinstance(legacy, list), (kind, code, legacy)
        assert len(legacy) <= 200
        for bad in ('0', '-1', '1.5', 'abc', '+1', '1e2', '2147483648'):
            assert http('/' + kind + '/page?' + urlencode({'page': bad}))[0] == 400, (kind, bad)
        for bad in ('0', '-1', '101', '1.5', 'abc', '+1', '2147483648'):
            assert http('/' + kind + '/page?' + urlencode({'pageSize': bad}))[0] == 400, (kind, bad)

    for status in ('ALL', 'QUEUED', 'RUNNING', 'CANCELLING', 'CANCELLED', 'SUCCEEDED', 'FAILED'):
        value = page('tasks', status=status, pageSize=100)
        assert_page(value, 1, 100)
        if status != 'ALL':
            assert all(item['status'] == status for item in value['items']), (status, value)
    for bad in ('succeeded', 'cancelled', 'UNKNOWN', 'ALL '):
        assert http('/tasks/page?' + urlencode({'status': bad}))[0] == 400, bad
    cancelled = {item['id'] for item in page('tasks', status='CANCELLED', pageSize=100)['items']}
    assert set(expected_tasks).issubset(cancelled), (expected_tasks, cancelled)

    first = page('datasets', q=prefix, page=1, pageSize=2)
    second = page('datasets', q=prefix, page=2, pageSize=2)
    assert_page(first, 1, 2)
    assert_page(second, 2, 2)
    assert first['total'] == second['total'] == len(datasets) == 4
    assert {item['id'] for item in first['items'] + second['items']} == {item['id'] for item in datasets}
    literal = page('datasets', q=prefix + '-%_', pageSize=100)
    assert literal['total'] == 1 and literal['items'][0]['id'] == datasets[0]['id'], literal
    assert page('datasets', q='  ' + prefix + '  ', pageSize=100)['total'] == 4
    missing = page('datasets', q='absent-' + uuid.uuid4().hex, page=1, pageSize=2)
    assert_page(missing, 1, 2)
    assert missing['total'] == missing['totalPages'] == 0 and missing['items'] == [], missing
    assert http('/datasets/page?' + urlencode({'q': 'x' * 201}))[0] == 400
    print('PASS pagination: default/filtered pages, stable created_at+id ordering, no cross-page '
          'duplicates, out-of-range/empty envelopes, literal %/_ name search, trimmed query, '
          'strict numeric/status/length validation and legacy array compatibility', flush=True)


def run():
    prefix = 'cancellation-' + uuid.uuid4().hex[:12]
    columns = ['id', 'name', 'age', 'email', 'region', 'token']
    raw = csv_bytes(columns, [
        ['1', 'Alice', '18', 'a@example.test', 'east', 'A01'],
        ['1', 'AliceDuplicate', '18', 'b@example.test', 'west', 'A02'],
        ['2', '\\N', '19', 'c@example.test', 'east', 'A03'],
        ['3', '', '20', 'd@example.test', 'west', 'A04'],
        ['4', ' \\N ', '21', 'e@example.test', 'east', 'A05'],
    ])
    code, source_data = upload(raw, prefix + '-%_literal.csv')
    assert code == 200, (code, source_data)
    heavy_raw = csv_bytes(columns, [
        [str(index // 2), 'User' + str(index), str(index % 121),
         'u' + str(index) + '@example.test', 'east', 'A01'] for index in range(25000)
    ])
    code, heavy_data = upload(heavy_raw, prefix + '-heavy.csv')
    assert code == 200 and heavy_data['row_count'] == 25000, (code, heavy_data)
    datasets = [source_data, heavy_data]
    for suffix in ('plain', 'third'):
        code, data = upload(csv_bytes(columns, [['9', 'Synthetic', '18', 'x@example.test', 'east', 'A09']]),
                            prefix + '-' + suffix + '.csv')
        assert code == 200, (code, data)
        datasets.append(data)
    simple_rules = [{'type': 'MISSING', 'columns': ['name']}, {'type': 'DUPLICATE', 'columns': ['id']}]
    baseline = wait_for(task(source_data, simple_rules)['id'], {'SUCCEEDED'})
    baseline_metrics = {rule['type']: rule['metrics'] for rule in baseline['rules']}
    assert baseline_metrics['MISSING']['blankCount'] == 2, baseline
    assert baseline_metrics['DUPLICATE'] == {'duplicateGroups': 1, 'duplicateRows': 2, 'excessRows': 1}
    baseline_report = get('/tasks/' + baseline['id'] + '/report')
    post('/tasks/' + baseline['id'] + '/cancel', expected=409)
    post('/tasks/' + str(uuid.uuid4()) + '/cancel', expected=404)
    assert get('/datasets/' + source_data['id'])['derivedVersions'] == []

    heavy_rules = simple_rules + [
        {'type': 'TYPE', 'columns': ['age'], 'params': {'dataType': 'INTEGER'}},
        {'type': 'REGEX', 'columns': ['email'], 'params': {'pattern': '[a-z0-9]+@example\\.test'}},
        {'type': 'REGEX', 'columns': ['token'], 'params': {'pattern': 'A[0-9]{2}'}},
    ] + [{'type': 'RANGE', 'columns': ['age'], 'params': {'min': '0', 'max': str(120 + index)}}
         for index in range(26)]
    running = wait_for(task(heavy_data, heavy_rules)['id'], {'RUNNING'}, 90)
    if VERIFY_ENGINE:
        prove_spark_started(running['id'])
    queued = post('/tasks/' + baseline['id'] + '/govern',
                  {'dropMissing': True, 'deduplicate': True}, record=True)
    assert queued['status'] == 'QUEUED', queued
    fillers = [task(source_data, simple_rules) for _ in range(7)]
    assert all(item['status'] == 'QUEUED' for item in fillers), fillers
    assert get('/tasks/' + running['id'])['status'] == 'RUNNING'
    assert any(item['id'] == running['id'] for item in page('tasks', status='RUNNING', pageSize=100)['items'])
    queued_page = page('tasks', status='QUEUED', pageSize=100)
    assert {queued['id'], *(item['id'] for item in fillers)}.issubset(item['id'] for item in queued_page['items'])
    count_before = page('tasks', pageSize=1)['total']
    post('/tasks', {'datasetId': source_data['id'], 'rules': simple_rules}, expected=429)
    assert page('tasks', pageSize=1)['total'] == count_before, 'Full queue rejection inserted a task'
    queued = cancel(queued['id'])
    queued_report = assert_cancelled(queued, queued=True)
    survivor = task(source_data, simple_rules)
    assert survivor['status'] == 'QUEUED', survivor
    cancelled = {'queuedGovernance': queued}
    cancelled_reports = {'queuedGovernance': queued_report}
    for index, filler in enumerate(fillers):
        current = cancel(filler['id'])
        report = assert_cancelled(current, queued=True)
        cancelled['queueFiller' + str(index)] = current
        cancelled_reports['queueFiller' + str(index)] = report
    requested = cancel(running['id'])
    assert requested['status'] in ('CANCELLING', 'CANCELLED'), requested
    assert cancel(running['id'])['status'] in ('CANCELLING', 'CANCELLED')
    running = wait_for(running['id'], {'CANCELLED'}, 60)
    cancelled['running'] = running
    cancelled_reports['running'] = assert_cancelled(running)
    if VERIFY_ENGINE:
        assert not engine_alive(running['id']), 'CANCELLED still has a live Spark engine process'
    survivor = wait_for(survivor['id'], {'SUCCEEDED'})
    assert survivor['total_rows'] == baseline['total_rows'] == 5
    assert {rule['type']: rule['metrics'] for rule in survivor['rules']} == baseline_metrics
    assert get('/tasks/' + running['id']) == running, 'Cancelled worker later overwrote its state'
    print('PASS queued/running cancellation: full eight-slot queue rejects without enqueue; queued '
          'governance cancellation releases capacity immediately; repeated cancellation is idempotent; '
          'cancelled tasks have no partial SQL results or versions, next job succeeds', flush=True)

    rerun = wait_for(post('/tasks/' + queued['id'] + '/rerun', {}, record=True)['id'], {'SUCCEEDED'})
    assert rerun['dataset_id'] == source_data['id'] and rerun['total_rows'] == 5
    assert {rule['type']: rule['metrics'] for rule in rerun['rules']} == baseline_metrics
    assert rerun['snapshot']['parent_task_id'] == queued['id'] and rerun['governance'] is None
    rerun_report = get('/tasks/' + rerun['id'] + '/report')
    assert 'comparison' not in rerun_report and 'outputDataset' not in rerun_report, rerun_report
    # This new synthetic value would normalize into the reserved null marker.
    failed = wait_for(post('/tasks/' + baseline['id'] + '/govern',
                           {'normalizations': {'name': 'TRIM'}}, record=True)['id'], {'FAILED'})
    assert failed['error'] and failed['governance']['output_dataset_id'] is None, failed
    post('/tasks/' + failed['id'] + '/cancel', expected=409)
    assert get('/tasks/' + baseline['id']) == baseline, 'Successful source task/results changed'
    assert get('/tasks/' + baseline['id'] + '/report') == baseline_report
    assert download(source_data['id']) == raw and download(heavy_data['id']) == heavy_raw
    assert get('/datasets/' + source_data['id'])['derivedVersions'] == [], 'Cancelled/failed governance leaked a version'
    validate_paging(prefix, datasets, [item['id'] for item in cancelled.values()])

    # Root may use this dedicated synthetic cancelled row for recovery injection.
    # Excluding it from equality snapshots permits a distinct recovery reason.
    recovery_id = fillers[-1]['id']
    cancelled.pop('queueFiller6')
    cancelled_reports.pop('queueFiller6')
    fixture = {
        'prefix': prefix, 'datasets': datasets, 'sourceDataset': source_data, 'heavyDataset': heavy_data,
        'sourceCsvBase64': base64.b64encode(raw).decode('ascii'),
        'baseline': baseline, 'baselineReport': baseline_report,
        'baselinePreviews': {rule['id']: get('/tasks/' + baseline['id'] + '/rules/' + rule['id'] + '/preview')
                             for rule in baseline['rules']},
        'cancelledTasks': cancelled, 'cancelledReports': cancelled_reports,
        'allQueueTaskIds': [queued['id']] + [item['id'] for item in fillers],
        'survivor': survivor, 'survivorReport': get('/tasks/' + survivor['id'] + '/report'),
        'rerun': rerun, 'rerunReport': rerun_report,
        'failed': failed, 'failedReport': get('/tasks/' + failed['id'] + '/report'),
        'recoveryId': recovery_id, 'engineProcessVerified': VERIFY_ENGINE,
    }
    Path('.runtime/cancellation_paging.json').write_text(
        json.dumps(fixture, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'cancelledRunningTask': running['id'], 'cancelledQueuedTask': queued['id'],
                      'survivorTask': survivor['id'], 'rerunTask': rerun['id'],
                      'recoveryId': recovery_id, 'engineProcessVerified': VERIFY_ENGINE}), flush=True)


if __name__ == '__main__':
    try:
        run()
    finally:
        # Never cancel a task that was not created by this exact test process.
        for task_id in reversed(OWN_IDS):
            try:
                current = get('/tasks/' + task_id)
                if current['status'] in ('QUEUED', 'RUNNING', 'CANCELLING'):
                    cancel(task_id)
            except Exception as error:
                print('CLEANUP pending synthetic task', task_id, str(error)[:300], file=sys.stderr, flush=True)
