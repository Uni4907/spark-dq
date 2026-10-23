"""Read-only HTTP assertions after restarting the governance-transform service.

Requires the successful real-Spark acceptance fixture. Sends only GET requests,
creates no tasks or datasets, and does not rewrite the fixture or output files.
Run from repository root: timeout 120 python3 tests/governance_transforms_persist.py
"""
from e2e import BASE, LOCAL, http
from pathlib import Path
import csv
import hashlib
import io
import json


def get(path):
    code, value = http(path)
    assert code == 200, (path, code, value)
    return value


def download(dataset_id):
    with LOCAL.open(BASE + '/datasets/' + dataset_id + '/download', timeout=30) as response:
        return response.read()


def rows(raw):
    return list(csv.reader(io.StringIO(raw.decode('utf-8-sig'), newline='')))


def rule_key(rule):
    return (rule['type'], tuple(rule['columns']),
            json.dumps(rule.get('params', {}), sort_keys=True))


def run():
    fixture_path = Path('.runtime/governance_transforms.json')
    assert fixture_path.is_file(), 'Run successful governance_transforms.py acceptance first'
    saved = json.loads(fixture_path.read_text(encoding='utf-8'))
    task_keys = ('source', 'normalized', 'idempotent', 'filled', 'idempotentFill',
                 'composed', 'rerun', 'collisionSource')
    assert set(task_keys).issubset(saved), ('Incomplete fixture', list(saved))
    actual_tasks = {}
    reports = {}
    for key in task_keys:
        expected = saved[key]
        actual = get('/tasks/' + expected['id'])
        assert actual == expected, (key, 'Task/snapshot/policy/audit/results changed', actual)
        assert actual['status'] == 'SUCCEEDED', (key, actual)
        assert all(rule['result_id'] and rule['metrics'] is not None for rule in actual['rules'])
        report = get('/tasks/' + actual['id'] + '/report')
        assert report['task'] == expected, (key, 'Report task differs from saved evidence')
        actual_tasks[key] = actual
        reports[key] = report
    print('PASS restart task persistence: eight successful task details, immutable rule snapshots, '
          'parameters, parent references, result IDs/metrics, policies and audits match saved evidence', flush=True)

    # Upload responses provide the pre-governance input digest and preview.
    for key in ('sourceDataset', 'collisionSourceDataset'):
        expected = saved[key]
        current = get('/datasets/' + expected['id'])
        for field in ('id', 'name', 'columns', 'row_count', 'sha256', 'preview'):
            assert current[field] == expected[field], (key, field, current[field], expected[field])
        assert hashlib.sha256(download(expected['id'])).hexdigest() == expected['sha256'], key

    versions = {}
    csv_outputs = {}
    governance_keys = ('normalized', 'idempotent', 'filled', 'idempotentFill', 'composed')
    columns = saved['sourceDataset']['columns']
    for key in governance_keys:
        task = actual_tasks[key]
        governance = task['governance']
        policy = governance['policy']
        assert policy['policyVersion'] == 2 and isinstance(policy['fills'], dict)
        assert isinstance(policy['normalizations'], dict)
        assert governance['audit'] == saved[key]['governance']['audit']
        output_id = governance['output_dataset_id']
        version = get('/datasets/' + output_id)
        assert version['version']['parent_dataset_id'] == task['dataset_id'], (key, version)
        assert version['version']['governance_task_id'] == task['id'], (key, version)
        assert version['columns'] == columns and version['row_count'] == task['total_rows']
        parent = get('/datasets/' + task['dataset_id'])
        assert any(child['dataset_id'] == output_id for child in parent['derivedVersions']), (key, parent)
        assert reports[key]['outputDataset'] == version, (key, reports[key])
        raw = download(output_id)
        assert hashlib.sha256(raw).hexdigest() == version['sha256'], (key, version)
        content = rows(raw)
        assert content[0] == columns and len(content) - 1 == task['total_rows'], (key, content)
        assert all(len(row) == len(columns) for row in content), (key, content)
        versions[key] = version
        csv_outputs[key] = content[1:]

    # These frozen child snapshots captured each parent's actual output hash
    # before restart; checking only a current file against current metadata is
    # insufficient evidence that the original persisted content survived.
    for output_key, child_key in (('normalized', 'idempotent'), ('filled', 'idempotentFill'),
                                  ('composed', 'rerun')):
        child = saved[child_key]
        assert child['dataset_id'] == versions[output_key]['id'], (output_key, child_key)
        assert versions[output_key]['sha256'] == child['snapshot']['input_sha256'], (output_key, child)
    for parent, child in (('normalized', 'idempotent'), ('filled', 'idempotentFill')):
        assert sorted(csv_outputs[child]) == sorted(csv_outputs[parent]), (parent, child)
        audit = actual_tasks[child]['governance']['audit']
        assert all(operation['changedRows'] == 0 for operation in audit['transformations']), (child, audit)
        assert audit['removedRows'] == 0 and audit['droppedMissingRows'] == 0
        assert audit['deduplicatedRows'] == 0
    composed = actual_tasks['composed']
    fill_literal = composed['governance']['policy']['fills']['name']
    assert any(row[1] == fill_literal and '\n' in row[1] and '\\' in row[1]
               for row in csv_outputs['composed']), csv_outputs['composed']
    assert any('\n' in row[4] and '第二行' in row[4] for row in csv_outputs['composed'])
    assert any(row[1] == '\t' for row in csv_outputs['composed'])
    assert composed['governance']['audit']['droppedMissingRows'] == 1
    assert composed['governance']['audit']['deduplicatedRows'] == 1
    assert composed['total_rows'] == 5
    assert actual_tasks['rerun']['dataset_id'] == versions['composed']['id']
    assert actual_tasks['rerun']['governance'] is None
    print('PASS restart CSV/version persistence: original input digests/previews preserved; five '
          'version chains/downloads/checksums retained; prior child snapshots prove unchanged output '
          'hashes; multiline/quoted/backslash/Unicode/tab values and zero-change outputs survive', flush=True)

    by_id = {task['id']: task for task in actual_tasks.values()}
    for key in ('normalized', 'idempotent', 'filled', 'idempotentFill', 'composed', 'rerun'):
        task = actual_tasks[key]
        parent_id = task['snapshot']['parent_task_id']
        parent = by_id[parent_id]
        report = reports[key]
        assert report['sourceTaskId'] == parent_id, (key, report)
        assert report['beforeRows'] == parent['total_rows']
        assert report['afterRows'] == task['total_rows']
        before = {rule_key(rule): rule['metrics'] for rule in parent['rules']}
        after = {rule_key(rule): rule['metrics'] for rule in task['rules']}
        assert len(report['comparison']) == len(before) == len(after) == 7, (key, report)
        for comparison in report['comparison']:
            definition = rule_key(comparison)
            assert comparison['beforeMetrics'] == before[definition], (key, comparison)
            assert comparison['afterMetrics'] == after[definition], (key, comparison)
            metric = comparison['metric']
            assert comparison['before'] == before[definition][metric]
            assert comparison['after'] == after[definition][metric]
            assert comparison['reducedBy'] == comparison['before'] - comparison['after']
    print('PASS restart reports: six before/after reports retain all seven rule definitions/params, '
          'exact persisted metrics, comparison deltas, output references and transformation evidence', flush=True)
    print(json.dumps({'checkedTasks': len(actual_tasks), 'checkedVersions': len(versions),
                      'composedTask': composed['id'], 'outputDataset': versions['composed']['id'],
                      'readOnly': True}), flush=True)


if __name__ == '__main__':
    run()
