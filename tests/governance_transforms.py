"""Real HTTP + Spark acceptance for selective governance transformations.

Only new synthetic datasets/tasks are created. Source bytes and stored results
are checked after every success/failure; no historical dataset is changed.
Run from the repository root: timeout 1200 python3 tests/governance_transforms.py
"""
from e2e import BASE, LOCAL, http, upload, wait
from pathlib import Path
import csv
import hashlib
import io
import json


def call(path, body, expected=200):
    code, result = http(path, json.dumps(body, ensure_ascii=False).encode('utf-8'),
                        {'Content-Type': 'application/json'})
    assert code == expected, (path, code, result)
    return result


def done(task):
    result = wait(task['id'])
    assert result['status'] == 'SUCCEEDED', result
    return result


def downloaded(dataset_id):
    with LOCAL.open(BASE + '/datasets/' + dataset_id + '/download', timeout=30) as response:
        return response.read()


def csv_bytes(columns, rows):
    stream = io.StringIO(newline='')
    # Quoted empties stay empty strings; the reserved \\N token stays a null.
    writer = csv.writer(stream, lineterminator='\r\n', quoting=csv.QUOTE_ALL)
    writer.writerow(columns)
    writer.writerows(rows)
    return stream.getvalue().encode('utf-8')


def output_rows(task, columns):
    output = task['governance']['output_dataset_id']
    code, version = http('/datasets/' + output)
    assert code == 200 and version['columns'] == columns, (code, version)
    raw = downloaded(output)
    assert version['sha256'] == hashlib.sha256(raw).hexdigest(), version
    parsed = list(csv.reader(io.StringIO(raw.decode('utf-8-sig'), newline='')))
    assert parsed[0] == columns and len(parsed) - 1 == task['total_rows'], parsed
    assert version['row_count'] == task['total_rows']
    assert version['version']['parent_dataset_id'] == task['dataset_id'], version
    assert version['version']['governance_task_id'] == task['id'], version
    return parsed[1:]


def definitions(task):
    return sorted((r['type'], tuple(r['columns']),
                   json.dumps(r.get('params', {}), sort_keys=True)) for r in task['rules'])


def metric(task, kind, column):
    return next(r['metrics'] for r in task['rules']
                if r['type'] == kind and r['columns'] == [column])


def assert_policy(task, *, fills=None, normalizations=None, drop=False, dedup=False):
    expected = {'policyVersion': 2, 'dropMissing': drop, 'deduplicate': dedup,
                'fills': fills or {}, 'normalizations': normalizations or {}}
    assert task['governance']['policy'] == expected, task['governance']


def assert_audit(task, transformations, *, dropped=0, deduplicated=0):
    audit = task['governance']['audit']
    assert audit['inputRows'] == 7, audit
    assert audit['outputRows'] == task['total_rows'], audit
    assert audit['removedRows'] == 7 - task['total_rows'], audit
    assert audit['droppedMissingRows'] == dropped, audit
    assert audit['deduplicatedRows'] == deduplicated, audit
    assert audit['transformations'] == [
        {'column': column, 'operation': operation, 'changedRows': count}
        for column, operation, count in transformations
    ], audit


def assert_unchanged(dataset, task, raw):
    assert downloaded(dataset['id']) == raw, 'Original dataset bytes changed'
    code, current = http('/tasks/' + task['id'])
    assert code == 200 and current == task, 'Original persisted task/results changed'


def new_task(dataset_id, rules):
    return done(call('/tasks', {'datasetId': dataset_id, 'rules': rules}))


def govern(source, **policy):
    return done(call('/tasks/' + source['id'] + '/govern', policy))


def run():
    columns = ['key', 'name', 'age', 'email', 'note', 'untouched',
               'trim_text', 'upper_text', 'lower_text']
    source_rows = [
        [' a ', '   ', '\\N', ' ALICE@EXAMPLE.TEST ', 'quoted "x",\n第二行\\folder',
         ' KEEP ', ' Ab ', 'MiXeD', 'MiXeD'],
        ['A', '', '', 'bob@example.test', 'line2', '保留', 'x', 'up', 'LOW'],
        [' b ', '\\N', '   ', '\\N', 'backslash \\path\\file', '  ', '', '\\N', ''],
        [' B ', ' Actual ', '9', ' CAROL@EXAMPLE.TEST ', 'literal \\N text',
         '\\N', '\\N', 'abc', 'DEF'],
        ['c', 'Zoe', 'oops', 'bad-email', '中文🙂, literal \\N', '', ' \t ', '1', '1'],
        ['d', 'MAY', '200', 'd@example.test', 'tab\tinside', 'X', '   ', '', '\\N'],
        ['e', '\t', '3', 'e@example.test', 'last', 'tail ', 'wide \u3000', 'ok', 'OK'],
    ]
    raw = csv_bytes(columns, source_rows)
    code, dataset = upload(raw, 'synthetic-governance-transforms.csv')
    assert code == 200 and dataset['row_count'] == 7, (code, dataset)
    rules = [
        {'type': 'MISSING', 'columns': ['name']},
        {'type': 'MISSING', 'columns': ['age']},
        {'type': 'MISSING', 'columns': ['email']},
        {'type': 'DUPLICATE', 'columns': ['key']},
        {'type': 'TYPE', 'columns': ['age'], 'params': {'dataType': 'INTEGER'}},
        {'type': 'RANGE', 'columns': ['age'], 'params': {'min': '0', 'max': '120'}},
        {'type': 'REGEX', 'columns': ['email'], 'params': {'pattern': '[a-z]+@example\\.test'}},
    ]
    source = new_task(dataset['id'], rules)
    assert metric(source, 'MISSING', 'name')['blankCount'] == 3
    assert metric(source, 'MISSING', 'age')['blankCount'] == 3
    assert metric(source, 'MISSING', 'email')['blankCount'] == 1
    assert metric(source, 'DUPLICATE', 'key')['duplicateRows'] == 0
    assert metric(source, 'TYPE', 'age')['invalidCount'] == 1
    assert metric(source, 'RANGE', 'age')['invalidCount'] == 2
    assert metric(source, 'REGEX', 'email')['invalidCount'] == 3

    # Every invalid policy must be rejected synchronously, before a task exists.
    before = {task['id'] for task in http('/tasks')[1]}
    invalid = [
        {}, {'dropMissing': False, 'deduplicate': False},
        {'fills': {}, 'normalizations': {}}, {'fills': {'missing_column': 'x'}},
        {'normalizations': {'missing_column': 'TRIM'}},
        {'normalizations': {'name': 'trim'}}, {'normalizations': {'name': 'REPLACE'}},
        {'normalizations': {'name': ''}}, {'normalizations': {'name': 1}},
        {'normalizations': {'name': None}}, {'normalizations': {'name': True}},
        {'normalizations': {'name': ['TRIM']}}, {'normalizations': {'name': {}}},
        {'fills': {'name': 1}}, {'fills': {'name': None}}, {'fills': {'name': True}},
        {'fills': {'name': ['x']}}, {'fills': {'name': {}}},
        {'fills': {'name': ''}}, {'fills': {'name': '   '}},
        {'fills': {'name': '\t\n'}}, {'fills': {'name': '\u3000'}},
        {'fills': {'name': '\\N'}}, {'fills': {'name': 'x\x00y'}},
        {'fills': {'name': 'x' * 1001}}, {'fills': ['name']},
        {'normalizations': ['TRIM']},
        {'fills': {'name': 'x'}, 'policyVersion': 1},
        {'fills': {'name': 'x'}, 'policyVersion': 5},
        {'fills': {'name': 'x'}, 'unknownAction': True},
    ]
    for bad in invalid:
        call('/tasks/' + source['id'] + '/govern', bad, 400)
    assert {task['id'] for task in http('/tasks')[1]} == before, 'Rejected policy left queued tasks'
    print('PASS policy validation: unknown fields/actions, selected columns, map shapes/value types, '
          'blank/sentinel/NUL/overlong fills, unsupported versions and empty policy reject before enqueue', flush=True)

    normalizations = {'key': 'TRIM_UPPER', 'email': 'TRIM_LOWER', 'trim_text': 'TRIM',
                      'upper_text': 'UPPER', 'lower_text': 'LOWER'}
    normalized = govern(source, normalizations=normalizations)
    assert normalized['total_rows'] == 7
    assert_policy(normalized, normalizations=normalizations)
    normalized_rows = output_rows(normalized, columns)
    expected_normalized = [
        ['A', '   ', '\\N', 'alice@example.test', source_rows[0][4], ' KEEP ', 'Ab', 'MIXED', 'mixed'],
        ['A', '', '', 'bob@example.test', 'line2', '保留', 'x', 'UP', 'low'],
        ['B', '\\N', '   ', '\\N', source_rows[2][4], '  ', '', '\\N', ''],
        ['B', ' Actual ', '9', 'carol@example.test', 'literal \\N text', '\\N', '\\N', 'ABC', 'def'],
        ['C', 'Zoe', 'oops', 'bad-email', source_rows[4][4], '', '\t', '1', '1'],
        ['D', 'MAY', '200', 'd@example.test', 'tab\tinside', 'X', '', '', '\\N'],
        ['E', '\t', '3', 'e@example.test', 'last', 'tail ', 'wide \u3000', 'OK', 'ok'],
    ]
    assert sorted(normalized_rows) == sorted(expected_normalized), normalized_rows
    normalize_audit = [('email', 'TRIM_LOWER', 2), ('key', 'TRIM_UPPER', 6),
                       ('lower_text', 'LOWER', 4), ('trim_text', 'TRIM', 3),
                       ('upper_text', 'UPPER', 4)]
    assert_audit(normalized, normalize_audit)
    assert metric(normalized, 'DUPLICATE', 'key')['duplicateRows'] == 4
    assert metric(normalized, 'REGEX', 'email')['invalidCount'] == 1
    assert_unchanged(dataset, source, raw)
    print('PASS all five normalization actions: selected fields only; null/empty/tab/unselected '
          'values and quoted multiline/backslash/Unicode CSV survive; operation counts match', flush=True)

    normalized_bytes = downloaded(normalized['governance']['output_dataset_id'])
    idempotent = govern(normalized, normalizations=normalizations)
    assert_policy(idempotent, normalizations=normalizations)
    assert sorted(output_rows(idempotent, columns)) == sorted(expected_normalized)
    assert_audit(idempotent, [(column, operation, 0)
                             for column, operation, count in normalize_audit])
    assert definitions(idempotent) == definitions(normalized)
    for rule in normalized['rules']:
        assert metric(idempotent, rule['type'], rule['columns'][0]) == rule['metrics']
    assert downloaded(normalized['governance']['output_dataset_id']) == normalized_bytes
    assert http('/tasks/' + normalized['id'])[1] == normalized
    print('PASS idempotent normalization: a new independent version keeps all values/metrics, '
          'records zero changed rows per operation and preserves the prior version/task', flush=True)

    literal = 'Fallback "名",\n预设\\folder'
    fills = {'name': literal, 'age': '18'}
    filled = govern(source, fills=fills)
    assert_policy(filled, fills=fills)
    expected_filled = [list(row) for row in source_rows]
    for row in expected_filled[:3]:
        row[1] = literal
        row[2] = '18'
    assert sorted(output_rows(filled, columns)) == sorted(expected_filled)
    assert_audit(filled, [('age', 'FILL_MISSING', 3), ('name', 'FILL_MISSING', 3)])
    assert metric(filled, 'MISSING', 'name')['blankCount'] == 0
    assert metric(filled, 'MISSING', 'age')['blankCount'] == 0
    assert metric(filled, 'MISSING', 'email')['blankCount'] == 1
    assert metric(filled, 'TYPE', 'age')['invalidCount'] == 1
    assert metric(filled, 'RANGE', 'age')['invalidCount'] == 2
    assert_unchanged(dataset, source, raw)
    print('PASS selective literal fill: null/empty/ASCII spaces repaired; tab and nonblank values '
          'preserved; no silent repair of unrelated type/range/regex anomalies', flush=True)

    filled_bytes = downloaded(filled['governance']['output_dataset_id'])
    idempotent_fill = govern(filled, fills=fills)
    assert_policy(idempotent_fill, fills=fills)
    assert sorted(output_rows(idempotent_fill, columns)) == sorted(expected_filled)
    assert_audit(idempotent_fill, [('age', 'FILL_MISSING', 0), ('name', 'FILL_MISSING', 0)])
    for rule in filled['rules']:
        assert metric(idempotent_fill, rule['type'], rule['columns'][0]) == rule['metrics']
    assert downloaded(filled['governance']['output_dataset_id']) == filled_bytes
    assert http('/tasks/' + filled['id'])[1] == filled
    print('PASS idempotent fill: selected nonblank fields stay untouched, a new version '
          'records zero changes, prior output bytes and task remain unchanged', flush=True)

    composed_normalizations = dict(normalizations, name='TRIM_UPPER')
    composed = govern(source, fills=fills, normalizations=composed_normalizations,
                      dropMissing=True, deduplicate=True, policyVersion=2)
    assert_policy(composed, fills=fills, normalizations=composed_normalizations, drop=True, dedup=True)
    assert composed['total_rows'] == 5, composed
    composed_rows = output_rows(composed, columns)
    expected_composed = [list(expected_normalized[index]) for index in [0, 3, 4, 5, 6]]
    expected_composed[0][1:3] = [literal, '18']
    expected_composed[1][1] = 'ACTUAL'
    expected_composed[2][1] = 'ZOE'
    assert sorted(composed_rows) == sorted(expected_composed), composed_rows
    compose_audit = normalize_audit[:3] + [('name', 'TRIM_UPPER', 3)] + normalize_audit[3:]
    assert_audit(composed, compose_audit + [('age', 'FILL_MISSING', 3), ('name', 'FILL_MISSING', 3)],
                 dropped=1, deduplicated=1)
    for field in ['name', 'age', 'email']:
        assert metric(composed, 'MISSING', field)['blankCount'] == 0
    assert metric(composed, 'DUPLICATE', 'key')['duplicateRows'] == 0
    assert metric(composed, 'TYPE', 'age')['invalidCount'] == 1
    assert metric(composed, 'RANGE', 'age')['invalidCount'] == 2
    assert metric(composed, 'REGEX', 'email')['invalidCount'] == 1
    assert definitions(composed) == definitions(source)
    assert {r['result_id'] for r in source['rules']}.isdisjoint(r['result_id'] for r in composed['rules'])
    code, report = http('/tasks/' + composed['id'] + '/report')
    assert code == 200 and report['beforeRows'] == 7 and report['afterRows'] == 5, (code, report)
    assert report['outputDataset']['id'] == composed['governance']['output_dataset_id']
    assert report['task']['governance']['audit'] == composed['governance']['audit']
    assert len(report['comparison']) == 7
    assert any(row['type'] == 'REGEX' and row['before'] == 3 and row['after'] == 1
               for row in report['comparison']), report
    rerun = done(call('/tasks/' + composed['id'] + '/rerun', {}))
    assert rerun['dataset_id'] == composed['governance']['output_dataset_id']
    assert rerun['governance'] is None and rerun['total_rows'] == 5
    assert definitions(rerun) == definitions(composed)
    for rule in composed['rules']:
        assert metric(rerun, rule['type'], rule['columns'][0]) == rule['metrics'], rerun
    assert_unchanged(dataset, source, raw)
    print('PASS normalization -> fill -> missing drop -> business-key dedup order: 7 -> 5; '
          'fill literals are not normalized, new version/lineage/checksum, immutable rules, '
          'saved-file independent rerun and seven-rule comparison report', flush=True)

    # A transform must fail rather than serialize a literal as the reserved null token.
    collision_raw = csv_bytes(['id', 'value'], [['1', ' \\N '], ['2', '\\n']])
    code, collision_dataset = upload(collision_raw, 'synthetic-sentinel-collision.csv')
    assert code == 200, (code, collision_dataset)
    collision_source = new_task(collision_dataset['id'], [{'type': 'MISSING', 'columns': ['value']}])
    for action in ['TRIM', 'UPPER']:
        failed = wait(call('/tasks/' + collision_source['id'] + '/govern',
                           {'normalizations': {'value': action}})['id'])
        assert failed['status'] == 'FAILED', failed
        assert failed['error'] and ('\\N' in failed['error'] or '保留' in failed['error']
                                    or 'sentinel' in failed['error'].lower()
                                    or 'reserved csv null marker' in failed['error'].lower()), failed
        assert failed['governance']['output_dataset_id'] is None, failed
        assert failed['governance']['audit'] is None, failed
        assert all(rule['result_id'] is None and rule['metrics'] is None for rule in failed['rules']), failed
        assert_unchanged(collision_dataset, collision_source, collision_raw)
    assert_unchanged(dataset, source, raw)
    print('PASS normalization sentinel collision: TRIM and UPPER fail with clear errors, '
          'no output version or SQL result rows, original bytes/results preserved', flush=True)

    fixture = {'sourceDataset': dataset, 'source': source, 'normalized': normalized,
               'idempotent': idempotent,
               'filled': filled, 'idempotentFill': idempotent_fill,
               'composed': composed, 'rerun': rerun,
               'collisionSourceDataset': collision_dataset, 'collisionSource': collision_source}
    Path('.runtime/governance_transforms.json').write_text(
        json.dumps(fixture, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'sourceTask': source['id'], 'governanceTask': composed['id'],
                      'outputDataset': composed['governance']['output_dataset_id'],
                      'rerunTask': rerun['id'], 'totalRows': composed['total_rows'],
                      'durationMs': composed['duration_ms']}), flush=True)


if __name__ == '__main__':
    run()
