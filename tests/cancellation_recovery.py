"""Maintenance-window recovery acceptance for a test-owned cancellation row.

Requires cancellation_paging.py and current regression fixtures. Refuses any
active task. Stops only this project's containers without removing data volumes,
injects CANCELLING only into the dedicated synthetic cancelled recovery ID,
restarts, and runs read-only persistence checks. Requires Docker permission.
Run from repository root: timeout 600s python3 tests/cancellation_recovery.py
"""
from pathlib import Path
import json,re,subprocess,sys,time
root=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(root/'tests'))
from e2e import http
compose=['docker','compose','--env-file','.env','-f','infrastructure/compose.yaml']
def command(args,seconds=120,**kwargs):
 return subprocess.run(args,cwd=root,check=True,timeout=seconds,**kwargs)
def healthy():
 end=time.monotonic()+90
 while time.monotonic()<end:
  try:
   if http('/health')[0]==200:return
  except OSError:pass
  time.sleep(1)
 raise AssertionError('Backend did not become healthy after bounded restart')
def checks(path,extra=False):
 scripts=[['tests/cancellation_persist.py']+(['--require-recovery'] if extra else []),
          ['tests/governance_persist.py'],['tests/rules_persist.py'],
          ['tests/governance_transforms_persist.py'],['tests/snapshots_persist.py']]
 with path.open('w',encoding='utf-8') as log:
  for script in scripts:
   result=command(['python3']+script,90,capture_output=True,text=True)
   print(result.stdout,end='',flush=True);log.write(result.stdout)
   if result.stderr:print(result.stderr,file=sys.stderr);log.write(result.stderr)
fixture=json.loads((root/'.runtime/cancellation_paging.json').read_text())
for status in ['QUEUED','RUNNING','CANCELLING']:
 code,page=http('/tasks/page?status='+status)
 assert code==200 and page['total']==0, 'Active tasks; defer restart'
checks(root/'.runtime/reliability-persist-before.log')
task_id=fixture['recoveryId'];dataset=fixture['sourceDataset']
assert re.fullmatch(r'[0-9a-f-]{36}',task_id) and re.fullmatch(r'[0-9a-f-]{36}',dataset['id'])
assert re.fullmatch(r'cancellation-[0-9a-f]{12}-%_literal[.]csv',dataset['name'])
code,before=http('/tasks/'+task_id)
assert code==200 and before['status']=='CANCELLED' and before['dataset_id']==dataset['id']
assert task_id in fixture['allQueueTaskIds']
assert all(r['result_id'] is None for r in before['rules'])
# Only this test-owned cancelled row is changed while its backend is stopped.
# Database credentials remain inside the MySQL container, never in output/argv.
sql=("UPDATE tasks t JOIN datasets d ON t.dataset_id=d.id "
     "SET t.status='CANCELLING',t.error='acceptance simulated pending cancellation',t.finished_at=NULL "
     "WHERE t.id='"+task_id+"' AND t.status='CANCELLED' AND t.dataset_id='"+dataset['id']+
     "' AND d.name='"+dataset['name']+"';\nSELECT ROW_COUNT();\n")
command(compose+['stop','backend'],90)
try:
 mysql=['docker','exec','-i','spark-dq-mysql-1','sh','-c',
        'export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; exec mysql --user=root --database="$MYSQL_DATABASE" --batch --skip-column-names']
 result=command(mysql,20,input=sql,capture_output=True,text=True)
 assert result.stdout.strip()=='1', ('Expected exactly one synthetic row',result.stdout)
 command(compose+['stop'],90)
finally:
 command(compose+['up','-d'],120)
healthy()
checks(root/'.runtime/reliability-persist-after.log',extra=True)
after=http('/tasks/'+task_id)[1]
assert after['status']=='CANCELLED' and '重启' in after['error']
evidence={'taskId':task_id,'datasetId':dataset['id'],'testOwned':True,
          'before':before,'simulatedStatus':'CANCELLING','after':after,
          'note':'Only dedicated synthetic fixture row changed while backend stopped; all data volumes retained.'}
(root/'.runtime/cancellation-recovery-experiment.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding='utf-8')
print('PASS actual CANCELLING restart recovery: exactly one dedicated synthetic row injected while backend stopped; recovered CANCELLED with explicit reason; prior data/results/reports unchanged.',flush=True)
