"""Real Spark acceptance for template revisions, parameter snapshots and advanced rules."""
from e2e import http,upload,wait
from pathlib import Path
import json,uuid

def call(path,body=None,method='POST',expected=200):
    code,result=http(path,None if body is None else json.dumps(body).encode(),{'Content-Type':'application/json'},method)
    assert code==expected,(code,result)
    return result
def done(task):
    task=wait(task['id']);assert task['status']=='SUCCEEDED',task
    return task
def definitions(task):
    return sorted((r['type'],tuple(r['columns']),json.dumps(r.get('params',{}),sort_keys=True)) for r in task['rules'])
def metrics(task):
    return {(r['type'],tuple(r['columns'])):r['metrics'] for r in task['rules']}
def row(task,type,column):
    return next(r for r in task['rules'] if r['type']==type and r['columns']==[column])

raw=b'id,age,amount,joined,code\n1,18,10.00,2024-02-29,A01\n1,200,99.01,2023-02-29,bad\n3,oops,abc,2024-01-01,A03\n4,\\N,\\N,\\N,\\N\n5,"","","",""\n6,"   ","   ","   ","   "\n7,-1,-0.1,2024-2-01,A07\n8,9223372036854775808,1.00000000001,not-a-date,A08\n9,120,99,2024-12-31,"A09\n"\n10,0,0,2024-01-01,A10\n'
code,data=upload(raw,'advanced-rules.csv');assert code==200 and data['row_count']==10,(code,data)
def rule(type,column,**params):return {'type':type,'columns':[column],'params':params}
rules=[rule('MISSING','age'),rule('DUPLICATE','id'),rule('TYPE','age',dataType='INTEGER'),rule('TYPE','amount',dataType='DECIMAL'),rule('TYPE','joined',dataType='DATE'),rule('RANGE','age',min='0',max='120'),rule('RANGE','amount',min='0',max='99'),rule('REGEX','code',pattern='A[0-9]{2}')]
template=call('/rule-templates',{'name':'Synthetic profile '+uuid.uuid4().hex[:8],'rules':rules})
tid=template['template_id'];assert template['revision']==1 and len(template['rules'])==8,template
task=done(call('/tasks',{'datasetId':data['id'],'templateId':tid,'templateRevision':1}))
assert task['template']['template_id']==tid and task['template']['revision']==1
m=metrics(task)
assert m['MISSING',('age',)]['blankCount']==3
for kind,field,count in [('TYPE','age',2),('TYPE','amount',2),('TYPE','joined',3),('RANGE','age',4),('RANGE','amount',4),('REGEX','code',2)]:
    metric=m[kind,(field,)]
    assert metric['invalidCount']==count and metric['checkedCount']==7 and metric['skippedBlankCount']==3,(kind,field,metric)
    r=row(task,kind,field);code,preview=http('/tasks/'+task['id']+'/rules/'+r['id']+'/preview')
    assert code==200 and len(preview)==count,(kind,field,preview)
assert m['RANGE',('age',)]['nonNumericCount']==1 and m['RANGE',('amount',)]['nonNumericCount']==2
assert m['DUPLICATE',('id',)]['duplicateRows']==2
print('PASS real Spark advanced rules: integer overflow, decimal precision, leap dates, inclusive ranges, nonnumeric values, strict full regex and separate blank counts/previews',flush=True)

updated=json.loads(json.dumps(rules));updated[5]['params']['max']='30'
v2=call('/rule-templates/'+tid,{'name':template['name']+' v2','rules':updated,'expectedRevision':1},'PUT')
assert v2['revision']==2
call('/rule-templates/'+tid,{'name':'stale','rules':rules,'expectedRevision':1},'PUT',409)
assert len(http('/rule-templates/'+tid+'/versions')[1])==2
assert http('/rule-templates/'+tid+'?revision=1')[1]['rules']==template['rules']
latest=done(call('/tasks',{'datasetId':data['id'],'templateId':tid}))
assert latest['template']['revision']==2 and row(latest,'RANGE','age')['metrics']['invalidCount']==5,latest
assert http('/tasks/'+task['id'])[1]==task,'Template edit changed historical task'
call('/rule-templates/'+tid+'?revision=1',method='DELETE',expected=409)
archived=call('/rule-templates/'+tid+'?revision=2',method='DELETE')
assert archived['archived'] and all(t['id']!=tid for t in http('/rule-templates')[1])
call('/tasks',{'datasetId':data['id'],'templateId':tid},expected=409)
rerun=done(call('/tasks/'+task['id']+'/rerun',{}))
assert definitions(rerun)==definitions(task) and metrics(rerun)==metrics(task) and rerun['template']['revision']==1
comparison=http('/tasks/'+rerun['id']+'/report')[1]['comparison']
assert len(comparison)==8 and all(r['reducedBy']==0 for r in comparison)
print('PASS immutable revisions: optimistic conflicts, historical revision reads, archive retains snapshots, rerun keeps revision 1 and all parameters',flush=True)

governed=done(call('/tasks/'+task['id']+'/govern',{'dropMissing':True,'deduplicate':True}))
assert governed['total_rows']==6 and governed['template']['revision']==1
assert definitions(governed)==definitions(task)
assert row(governed,'TYPE','age')['metrics']['invalidCount']==2,'Governance must not silently repair invalid types'
assert row(governed,'REGEX','code')['metrics']['invalidCount']==1
assert len(http('/tasks/'+governed['id']+'/report')[1]['comparison'])==8
print('PASS advanced-rule governance/recheck/report: 10 -> 6 rows; selected policies do not silently clear remaining advanced-rule anomalies',flush=True)

before={t['id'] for t in http('/tasks')[1]}
invalid=[[],[rule('REGEX','code',pattern='[')],[rule('RANGE','age',min=2,max=1)],[rule('RANGE','age',max='0.00000000001')],[rule('TYPE','age',dataType=None)],[rule('TYPE','unknown',dataType='INTEGER')],[rule('MISSING','age',extra=1)]]
for bad in invalid:call('/tasks',{'datasetId':data['id'],'rules':bad},expected=400)
call('/tasks',{'datasetId':data['id'],'templateId':tid,'rules':rules},expected=400)
call('/tasks',{'datasetId':data['id'],'templateId':str(uuid.uuid4())},expected=404)
bad_template=rules+[rule('REGEX','code',pattern='[')]
call('/rule-templates',{'name':'invalid','rules':bad_template},expected=400)
code,other=upload(b'id,other\n1,x\n','incompatible-template.csv');assert code==200,other
active=call('/rule-templates',{'name':'Compatibility '+uuid.uuid4().hex[:8],'rules':rules})
call('/tasks',{'datasetId':other['id'],'templateId':active['template_id']},expected=400)
assert {t['id'] for t in http('/tasks')[1]}==before,'Rejected inputs left queued tasks'
solo=done(call('/tasks',{'datasetId':data['id'],'rules':[rule('TYPE','age',dataType='INTEGER')]}))
call('/tasks/'+solo['id']+'/govern',{'dropMissing':True,'deduplicate':False},expected=400)
call('/tasks/'+solo['id']+'/govern',{'dropMissing':False,'deduplicate':True},expected=400)
Path('.runtime/rule_templates.json').write_text(json.dumps({'template':template,'archivedTemplate':archived,'source':task,'latest':latest,'rerun':rerun,'governed':governed,'activeTemplate':active}))
print('PASS invalid rules/schema/policy rejection before enqueue; direct rule API also works',flush=True)
print(json.dumps({'taskId':task['id'],'templateId':tid,'governedTask':governed['id'],'durationMs':task['duration_ms']}),flush=True)
