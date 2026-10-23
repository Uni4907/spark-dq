"""Real HTTP + Spark checks for additive snapshots and rerun lineage."""
from e2e import upload, create, wait, http
import hashlib, json, uuid
from pathlib import Path

def rerun(task_id, body=None, expected=200):
    code, result = http('/tasks/'+task_id+'/rerun', json.dumps(body or {}).encode(), {'Content-Type':'application/json'})
    assert code == expected, (code, result)
    return result

def definitions(task):
    return sorted((r['type'], tuple(r['columns'])) for r in task['rules'])

def metrics(task):
    return {r['type']+':'+','.join(r['columns']):r['metrics'] for r in task['rules']}

raw=b'id,name,email\n1,\\N,a@example.test\n1,A,\\N\n2,B,b@example.test\n'
code, data=upload(raw, 'rerun-source.csv'); assert code==200, data
original=create(data['id'], ['name','email'], ['id'])
assert original['snapshot']['input_sha256']==hashlib.sha256(raw).hexdigest()
assert original['snapshot']['semantics_version']=='csv-v2'
assert original['snapshot']['parent_task_id'] is None
rerun(original['id'], expected=409)
original=wait(original['id']); assert original['status']=='SUCCEEDED', original

child=rerun(original['id'])
assert child['id']!=original['id']
assert child['snapshot']['parent_task_id']==original['id']
assert child['snapshot']['input_sha256']==original['snapshot']['input_sha256']
assert definitions(child)==definitions(original)
assert {r['id'] for r in child['rules']}.isdisjoint(r['id'] for r in original['rules'])
child=wait(child['id']); assert child['status']=='SUCCEEDED', child
assert metrics(child)==metrics(original)
assert {r['result_id'] for r in child['rules']}.isdisjoint(r['result_id'] for r in original['rules'])
print('PASS original-data rerun: frozen rules, same metrics, distinct task/rule/result IDs and parent lineage',flush=True)

code, clean=upload(b'id,name,email\n1,A,a@example.test\n2,B,b@example.test\n','rerun-governed.csv'); assert code==200,clean
clean_child=wait(rerun(original['id'], {'datasetId':clean['id']})['id'])
assert clean_child['status']=='SUCCEEDED' and clean_child['total_rows']==2, clean_child
assert clean_child['dataset_id']==clean['id'] and definitions(clean_child)==definitions(original)
assert clean_child['snapshot']['input_sha256']==clean['sha256']
assert all(v==0 for rule in clean_child['rules'] for v in rule['metrics'].values())
assert http('/tasks/'+original['id'])[1]==original, 'Original task was changed'
code,incompatible=upload(b'id,other\n1,x\n','rerun-incompatible.csv'); assert code==200,incompatible
before={t['id'] for t in http('/tasks')[1]}
rerun(original['id'], {'datasetId':incompatible['id']},400)
rerun(original['id'], {'datasetId':str(uuid.uuid4())},404)
rerun(str(uuid.uuid4()),expected=404)
assert {t['id'] for t in http('/tasks')[1]}==before, 'Rejected requests created tasks'
print('PASS new-data rerun: zero anomalies after synthetic governance; missing columns rejected before enqueue; history preserved',flush=True)

# Pre-upgrade tasks have rules but deliberately no invented historical snapshots.
legacy=next((t for t in http('/tasks')[1] if t['status'] in ('SUCCEEDED','FAILED') and http('/tasks/'+t['id'])[1]['snapshot'] is None),None)
if legacy:
    old=http('/tasks/'+legacy['id'])[1]
    migrated=wait(rerun(old['id'])['id'])
    assert migrated['status']=='SUCCEEDED',migrated
    assert definitions(migrated)==definitions(old)
    assert http('/tasks/'+old['id'])[1]['snapshot'] is None
    print('PASS legacy persisted rules can rerun without backfilling historical snapshots',flush=True)
else:
    print('SKIP legacy rerun: no pre-upgrade task present',flush=True)
Path('.runtime/rerun.json').write_text(json.dumps({'source':original,'same':child,'governed':clean_child}))
print('PASS snapshot fixture saved for restart verification',flush=True)
