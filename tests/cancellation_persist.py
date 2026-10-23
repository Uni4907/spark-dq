"""Read-only restart assertions for cancellation, results, files and paging.

Requires .runtime/cancellation_paging.json. Sends only GET requests and writes
nothing. --require-recovery checks root's dedicated synthetic CANCELLING-row
restart experiment; the ordinary cancellation snapshot remains unchanged.
Run: timeout 120 python3 tests/cancellation_persist.py [--require-recovery]
"""
from e2e import BASE, LOCAL, http
from pathlib import Path
from urllib.parse import urlencode
import base64
import hashlib
import json
import sys


def get(path):
    code, value = http(path)
    assert code == 200, (path, code, value)
    return value


def download(dataset_id):
    with LOCAL.open(BASE + '/datasets/' + dataset_id + '/download', timeout=30) as response:
        return response.read()


def assert_cancelled(actual):
    assert actual['status'] == 'CANCELLED' and actual['finished_at'], actual
    assert actual['duration_ms'] is not None and actual['total_rows'] is None, actual
    assert actual['spark_version'] is None
    assert all(rule['result_id'] is None and rule['metrics'] is None for rule in actual['rules']), actual
    if actual['governance']:
        assert actual['governance']['output_dataset_id'] is None and actual['governance']['audit'] is None
    report = get('/tasks/' + actual['id'] + '/report')
    assert report['task'] == actual
    assert 'comparison' not in report and 'outputDataset' not in report, report
    assert 'beforeRows' not in report and 'afterRows' not in report, report
    return report


def run():
    path = Path('.runtime/cancellation_paging.json')
    assert path.is_file(), 'Run successful cancellation_paging.py acceptance first'
    fixture = json.loads(path.read_text(encoding='utf-8'))
    cancelled = fixture['cancelledTasks']
    assert len(cancelled) == 8 and len(fixture['allQueueTaskIds']) == 8, fixture.keys()
    for name, expected in cancelled.items():
        actual = get('/tasks/' + expected['id'])
        assert actual == expected, (name, 'Cancelled task changed after restart', actual)
        report = assert_cancelled(actual)
        assert report == fixture['cancelledReports'][name], (name, report)
        if name != 'running':
            assert actual['started_at'] is None, (name, actual)
        for rule in actual['rules'][:1]:
            assert http('/tasks/' + actual['id'] + '/rules/' + rule['id'] + '/preview')[0] == 404

    recovery = get('/tasks/' + fixture['recoveryId'])
    assert_cancelled(recovery)
    assert recovery['id'] in fixture['allQueueTaskIds']
    assert recovery['id'] not in {value['id'] for value in cancelled.values()}
    if '--require-recovery' in sys.argv:
        assert recovery['error'] and '重启' in recovery['error'], recovery
    print('PASS restart cancellation persistence: eight frozen task/snapshot/report records '
          'and dedicated recovery ID remain CANCELLED, with no SQL result rows, anomaly previews, '
          'governance versions or invented comparisons', flush=True)

    for key in ('baseline', 'survivor', 'rerun', 'failed'):
        expected = fixture[key]
        actual = get('/tasks/' + expected['id'])
        assert actual == expected, (key, 'Finished task/results changed', actual)
        report = get('/tasks/' + expected['id'] + '/report')
        assert report == fixture[key + 'Report'], (key, report)
    baseline = fixture['baseline']
    for rule_id, expected in fixture['baselinePreviews'].items():
        actual = get('/tasks/' + baseline['id'] + '/rules/' + rule_id + '/preview')
        assert actual == expected, (rule_id, actual)
    assert fixture['rerun']['snapshot']['parent_task_id'] == cancelled['queuedGovernance']['id']
    assert fixture['rerun']['governance'] is None and fixture['rerun']['status'] == 'SUCCEEDED'
    print('PASS restart success/failure persistence: original results and anomaly previews, '
          'subsequent successful work, cancelled-task rerun, failed governance and saved reports retained', flush=True)

    for expected in fixture['datasets']:
        actual = get('/datasets/' + expected['id'])
        for field in ('id', 'name', 'columns', 'row_count', 'sha256', 'preview'):
            assert actual[field] == expected[field], (expected['id'], field, actual)
        raw = download(expected['id'])
        assert hashlib.sha256(raw).hexdigest() == expected['sha256']
        assert actual['derivedVersions'] == [], actual
        if expected['id'] == fixture['sourceDataset']['id']:
            assert raw == base64.b64decode(fixture['sourceCsvBase64'], validate=True)
    query = urlencode({'q': fixture['prefix'], 'page': 1, 'pageSize': 2})
    first = get('/datasets/page?' + query)
    second = get('/datasets/page?' + urlencode({'q': fixture['prefix'], 'page': 2, 'pageSize': 2}))
    assert first['total'] == second['total'] == 4 and first['totalPages'] == second['totalPages'] == 2
    expected_ids = {value['id'] for value in fixture['datasets']}
    records = first['items'] + second['items']
    assert {record['id'] for record in records} == expected_ids and len(records) == len(expected_ids)
    keys = [(record['created_at'], record['id']) for record in records]
    assert keys == sorted(keys, reverse=True), records
    filtered = get('/tasks/page?' + urlencode({'status': 'CANCELLED', 'pageSize': 100}))
    assert all(item['status'] == 'CANCELLED' for item in filtered['items']), filtered
    assert filtered['page'] == 1 and filtered['pageSize'] == 100
    assert filtered['total'] >= len(cancelled) + 1
    for endpoint in ('/tasks', '/datasets'):
        assert isinstance(get(endpoint), list)
    print('PASS restart files/pagination: four original CSV digests/previews/bytes preserved, '
          'no derived versions leaked, dataset search pages/order/counts survive, '
          'cancelled filtering and legacy list endpoints remain available', flush=True)
    print(json.dumps({'checkedCancelled': len(cancelled) + 1, 'checkedFinished': 4,
                      'checkedDatasets': 4, 'recoveryId': recovery['id'],
                      'recoveryReasonVerified': '--require-recovery' in sys.argv,
                      'readOnly': True}), flush=True)


if __name__ == '__main__':
    run()
