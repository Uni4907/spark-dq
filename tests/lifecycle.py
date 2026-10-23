"""Run inside WSL with permission for project-only Docker operations."""
from e2e import http,create,wait
import json, subprocess,time,hashlib
from pathlib import Path
snapshot=json.loads(Path('.runtime/e2e.json').read_text())
compose=['docker','compose','--env-file','.env','-f','infrastructure/compose.yaml']
def healthy():
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        try:
            if http('/health')[0]==200:return
        except Exception:pass
        time.sleep(1)
    raise AssertionError('Backend not healthy')
def docker(*args):subprocess.run(['docker',*args],check=True)
data=snapshot['cleanDataset'];path=data['file_path']
assert Path(path).parent==Path('/data/raw') and Path(path).suffix=='.csv'
docker('exec','spark-dq-backend-1','mv',path,path+'.offline')
try:
    failed=wait(create(data['id'],['name'],['id'])['id'])
    assert failed['status']=='FAILED' and failed['error'] and failed['duration_ms']>0,failed
    assert 'PATH_NOT_FOUND' in failed['error'] or 'Path does not exist' in failed['error'],failed
    assert all(r['result_id'] is None for r in failed['rules'])
    print('PASS failure: unavailable synthetic input -> FAILED + actual Spark cause + duration; no partial SQL results',flush=True)
finally:docker('exec','spark-dq-backend-1','mv',path+'.offline',path)
first=create(data['id'],['name'],['id']);second=create(data['id'],['name'],['id'])
assert http('/tasks/'+second['id'])[1]['status']=='QUEUED'
subprocess.run(compose+['kill','--signal','SIGKILL','backend'],check=True)
subprocess.run(compose+['down'],check=True)
subprocess.run(compose+['up','-d'],check=True)
healthy()
for task in (first,second):
    restored=http('/tasks/'+task['id'])[1]
    assert restored['status']=='FAILED' and '重启中断' in restored['error'] and restored['duration_ms'] is not None,restored
original=snapshot['task'];code,persisted=http('/tasks/'+original['id'])
assert code==200 and persisted['status']=='SUCCEEDED' and persisted['total_rows']==original['total_rows']
assert persisted['rules']==original['rules']
d=snapshot['dataset'];assert http('/datasets/'+d['id'])[1]['sha256']==d['sha256']
for rule in persisted['rules']:
    code,samples=http(f"/tasks/{persisted['id']}/rules/{rule['id']}/preview");assert code==200 and samples
raw=subprocess.check_output(['docker','exec','spark-dq-backend-1','cat',d['file_path']])
assert hashlib.sha256(raw).hexdigest()==d['sha256']
print('PASS down/up: MySQL metadata/results, file volume/anomalies, original SHA-256 persist; queued/running jobs fail explicitly after restart',flush=True)
print(json.dumps({'failedTask':failed['id'],'interruptedTasks':[first['id'],second['id']],'persistedTask':persisted['id']},ensure_ascii=False),flush=True)
