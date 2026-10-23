"""Historical csv-v1 snapshots retain reader semantics on rerun."""
from e2e import http,wait
import json
old=None
for task in http('/tasks')[1]:
    if task['status']!='SUCCEEDED': continue
    detail=http('/tasks/'+task['id'])[1]
    if detail['snapshot'] and detail['snapshot']['semantics_version']=='csv-v1':
        old=detail;break
if old is None:
    print('SKIP csv-v1 compatibility: no historical snapshot in this database',flush=True)
else:
    code,body=http('/tasks/'+old['id']+'/govern',json.dumps({'dropMissing':True,'deduplicate':True}).encode(),{'Content-Type':'application/json'})
    assert code==409,(code,body)
    code,child=http('/tasks/'+old['id']+'/rerun',b'{}',{'Content-Type':'application/json'})
    assert code==200,(code,child)
    child=wait(child['id']);assert child['status']=='SUCCEEDED',child
    assert child['snapshot']['semantics_version']=='csv-v1' and child['total_rows']==old['total_rows']
    def metrics(task): return {r['type']+':'+','.join(r['columns']):r['metrics'] for r in task['rules']}
    assert metrics(child)==metrics(old),(child,old)
    print('PASS historical csv-v1: governance explicitly rejected, original reader semantics and metrics retained by rerun',flush=True)
