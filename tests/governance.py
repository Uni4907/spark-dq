"""Real Spark governance/version/recheck/report acceptance, using synthetic data only."""
from e2e import http,upload,create,wait,LOCAL,BASE
from pathlib import Path
import json,hashlib,uuid

def post(path,body,expected=200):
    code,data=http(path,json.dumps(body).encode(),{'Content-Type':'application/json'})
    assert code==expected,(code,data)
    return data
def govern(id,drop=True,dedup=True,expected=200):
    return post('/tasks/'+id+'/govern',{'dropMissing':drop,'deduplicate':dedup},expected)
def download(id):
    with LOCAL.open(BASE+'/datasets/'+id+'/download',timeout=20) as response:
        assert 'attachment' in response.headers['Content-Disposition']
        return response.read()
def check_done(task):
    task=wait(task['id']);assert task['status']=='SUCCEEDED',task
    return task
def rule_metrics(task):return {r['type']+':'+','.join(r['columns']):r['metrics'] for r in task['rules']}

raw=Path('datasets/known-issues.csv').read_bytes()
code,source=upload(raw,'governance-known.csv');assert code==200,source
original=check_done(create(source['id'],['name','email'],['id']))
before_ids={t['id'] for t in http('/tasks')[1]}
govern(original['id'],False,False,400)
govern(str(uuid.uuid4()),expected=404)
assert http('/tasks/'+original['id']+'/report')[0]==200
assert {t['id'] for t in http('/tasks')[1]}==before_ids

pending=govern(original['id'])
govern(pending['id'],expected=409)
assert http('/tasks/'+pending['id']+'/report')[0]==409
clean=check_done(pending)
g=clean['governance']; assert (g['input_rows'],g['output_rows'],g['removed_rows'])==(7,2,5),clean
assert clean['total_rows']==2 and all(v==0 for r in clean['rules'] for v in r['metrics'].values())
out_id=g['output_dataset_id'];assert out_id!=source['id']
code,version=http('/datasets/'+out_id);assert code==200,version
assert version['version']['parent_dataset_id']==source['id'] and version['version']['governance_task_id']==clean['id']
assert version['sha256']==hashlib.sha256(download(out_id)).hexdigest()
assert download(source['id'])==raw,'Original file changed'
assert http('/tasks/'+original['id'])[1]==original,'Original result changed'
assert any(v['dataset_id']==out_id for v in http('/datasets/'+source['id'])[1]['derivedVersions'])
report=http('/tasks/'+clean['id']+'/report')[1]
assert report['beforeRows']==7 and report['afterRows']==2 and report['outputDataset']['id']==out_id
assert sorted((r['before'],r['after'],r['reducedBy']) for r in report['comparison'])==[(2,0,2),(2,0,2),(3,0,3)],report
for rule in clean['rules']:assert http('/tasks/'+clean['id']+'/rules/'+rule['id']+'/preview')[1]==[]
print('PASS governance closed loop: 7 -> 2 rows, original bytes/results unchanged, new version/checksum/lineage, real-file recheck, three before/after metrics',flush=True)

dedup=check_done(govern(original['id'],False,True))
assert dedup['total_rows']==6 and rule_metrics(dedup)['MISSING:name']['blankCount']==3 and rule_metrics(dedup)['MISSING:email']['blankCount']==1
assert rule_metrics(dedup)['DUPLICATE:id']['duplicateRows']==0
again=check_done(post('/tasks/'+clean['id']+'/rerun',{}))
assert again['dataset_id']==out_id and again['total_rows']==2 and again['governance'] is None
chained=check_done(govern(clean['id'],False,True))
assert chained['governance']['input_rows']==2 and chained['governance']['output_rows']==2
assert http('/datasets/'+chained['governance']['output_dataset_id'])[1]['version']['parent_dataset_id']==out_id
print('PASS policy isolation and version chaining: dedup alone retains missing values; rerun/govern use output version as input',flush=True)

code,empty_source=upload(b'id,name\n1,\\N\n2,""\n3,"   "\n','governance-empty.csv');assert code==200,empty_source
empty_original=check_done(create(empty_source['id'],['name'],['id']))
empty=check_done(govern(empty_original['id'],True,False))
assert empty['total_rows']==0 and empty['governance']['removed_rows']==3,empty
empty_version=http('/datasets/'+empty['governance']['output_dataset_id'])[1]
assert empty_version['row_count']==0 and empty_version['preview']==[] and empty_version['columns']==['id','name']
empty_recheck=check_done(post('/tasks/'+empty['id']+'/rerun',{}))
assert empty_recheck['total_rows']==0
print('PASS all rows removed: valid zero-row version, retained schema, zero metrics and successful independent rerun',flush=True)

roundtrip=b'id,name,note\n1,Z,x\n1,"A ""quoted"",\nvalue",x\n2,"   ",space\n3,"",empty\n4,\\N,null\n'
code,rt=upload(roundtrip,'governance-roundtrip.csv');assert code==200,rt
rt_original=check_done(create(rt['id'],['name'],['id']))
rt_clean=check_done(govern(rt_original['id'],False,True))
rt_version=http('/datasets/'+rt_clean['governance']['output_dataset_id'])[1]
rows={r[0]:r for r in rt_version['preview']}
assert rows['1'][1]=='A "quoted",\nvalue' and rows['2'][1]=='   ' and rows['3'][1]=='' and rows['4'][1]=='\\N',rows
assert rule_metrics(rt_clean)['MISSING:name']=={'nullCount':1,'emptyCount':1,'whitespaceCount':1,'blankCount':3},rt_clean
assert rt_clean['total_rows']==4
rt_rerun=check_done(post('/tasks/'+rt_clean['id']+'/rerun',{}))
assert rule_metrics(rt_rerun)==rule_metrics(rt_clean)
assert download(rt['id'])==roundtrip
print('PASS RFC4180 serialization: deterministic representative, quotes/commas/newlines, whitespace, empty and null values preserved and rechecked',flush=True)

fixture={'source':original,'clean':clean,'dedup':dedup,'chained':chained,'empty':empty,'roundtrip':rt_clean}
Path('.runtime/governance.json').write_text(json.dumps(fixture))
print(json.dumps({'governanceTask':clean['id'],'outputDataset':out_id,'durationMs':clean['duration_ms']}),flush=True)
