#!/usr/bin/env python3
"""Same physical host: formal API local/Standalone IQR+FD acceptance."""
import contextlib,json,os,re,shutil,subprocess,sys,time,traceback,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'infrastructure'),str(ROOT/'tests')]
import storage as st,cluster_api_trial as api
from mysql_import_trial import Client,ready,validate_task,save,request
from iqr_acceptance import csv_bytes

def fixture():
 cases={c['name']:c['values'] for c in json.loads((ROOT/'tests/algorithm-independent-cases.json').read_text())['iqrCases']}
 cases.update(tiny=['.0000000001','.0000000002','.0000000003','.0000000004','.0000000005','.0000000006','.0000000007','.0000000100'],small=['1','2','100'],none=[None,'','   ','NaN','1e3','1.00000000001','10000000000000000000000000000'],suffix=['1\n','1\r\n','1\t','1 ',' 1','0','1','100'])
 base=json.loads((ROOT/'tests/algorithm-independent-cases.json').read_text())['fdRows'];rows=[dict(r) for r in base]+[dict(base[0]),dict(base[0])]
 for i,row in enumerate(rows):
  row.update({name:values[i] if i<len(values) else None for name,values in cases.items()});row['empty']=None;row['text']='中文\r\n换行\t"';row['day']='2026-10-21'
 rules=[dict(type='IQR',columns=[name]) for name in cases]+[dict(type='FD',columns=['x1','x2'],params={'dependentColumns':['y1','y2']}),dict(type='FD',columns=['empty'],params={'dependentColumns':['y1']}),dict(type='MISSING',columns=['empty']),dict(type='DUPLICATE',columns=['id']),dict(type='TYPE',columns=['day'],params={'dataType':'DATE'}),dict(type='REGEX',columns=['text'],params={'pattern':'.*'})]
 return rows,rules

def main():
 source=Path(sys.argv[1]).resolve();source.relative_to(ROOT/'.runtime');assert source.name=='backup'
 root=ROOT/'.runtime'/('t10-standalone-'+time.strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:6]);root.mkdir(mode=0o700)
 assert shutil.disk_usage(root).free>=8*1024**3
 available=int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')))*1024;assert available>=8*1024**3
 project=None;nodes=None;summary=dict(ok=False,scope='Single physical host Standalone distributed execution compatibility',checks=[],availableMemoryBytes=available)
 def check(message):summary['checks'].append(message);save(root/'progress.json',summary);print(message,flush=True)
 try:
  receipt=st.restore(source,root/'restored');project=st.Project(root/'restored/compose.yaml',root/'restored/.env');subprocess.run(['docker','update','--memory','3g','--memory-swap','3g','--cpus','2',project.service('backend')],check=True,stdout=subprocess.DEVNULL);subprocess.run(['docker','update','--memory','768m','--memory-swap','768m','--cpus','1',project.service('mysql')],check=True,stdout=subprocess.DEVNULL);c=Client(receipt['api']);ready(c);rows,rules=fixture();save(root/'fixture-rows.json',rows)
  data=c.upload(csv_bytes(list(rows[0]),rows),'standalone-IQR-FD-contract.csv');code,template=c.call('/rule-templates',{'name':'Standalone算法契约','rules':rules});assert code==200
  local=c.task({'datasetId':data['id'],'templateId':template['template_id'],'templateRevision':1});expected=validate_task(c,project.service('backend'),local,rows,root);check('local full anomaly multisets and independent Decimal/FD oracles')
  st.run(project.compose+['stop','backend']);config=root/'standalone.yaml';text=(root/'restored/compose.yaml').read_text();text=text.replace('      SPARK_LOCAL_IP: 127.0.0.1','''      SPARK_LOCAL_IP: 127.0.0.1
      SPARK_DQ_EXECUTION: standalone
      SPARK_MASTER: spark://master:7077
      SPARK_DQ_DRIVER_HOST: driver
      SPARK_DQ_MASTER_JSON_URL: http://master:8080/json/
      SPARK_DQ_WORKER_JSON_URLS: http://worker1:8081/json/,http://worker2:8081/json/
      SPARK_INPUT_PARTITIONS: 8
      SPARK_SHUFFLE_PARTITIONS: 2''')
  text=text.replace('    ports: ["127.0.0.1::8080"]','    networks: {default: {aliases: [driver]}}\n    ports: ["127.0.0.1::8080"]');assert 'aliases: [driver]' in text;config.write_text(text)
  project=st.Project(config,root/'restored/.env');nodes=api.Nodes(root,receipt['project']+'_default',receipt['filesVolume']);nodes.start();st.run(project.compose+['up','-d','--no-build','--pull','never']);c=Client('http://'+st.run(project.compose+['port','backend','8080']).decode().strip()+'/api');ready(c)
  remote=c.task({'datasetId':data['id'],'templateId':template['template_id'],'templateRevision':1});assert validate_task(c,project.service('backend'),remote,rows,root)==expected
  proof=json.loads(st.run(['docker','exec',project.service('backend'),'cat','/data/tasks/'+remote['id']+'/executor-evidence.json']));assert proof['applicationEnded'] and not proof['truncated'];assert {x['host'] for x in proof['executors']}=={'worker1','worker2'};assert all(x['successfulTasks']>0 and x['failedTasks']==0 for x in proof['executors']);save(root/'two-worker-proof.json',proof)
  assert remote['execution']['started']['applicationId'].startswith('app-');assert remote['clusterExecution']['cleanupObservation']=='CLEAR';check('same JAR/input/rules: Standalone full metrics, exact thresholds, complete anomaly multisets and both Workers execute tasks')
  changed=json.loads(json.dumps(rules));changed[0]['params']={'k':'2'};assert c.call('/rule-templates/'+template['template_id'],{'name':'更新不改变历史规则','rules':changed,'expectedRevision':1},'PUT')[0]==200
  replay=c.task({},'/tasks/'+remote['id']+'/rerun');assert validate_task(c,project.service('backend'),replay,rows,root)==expected;check('Standalone frozen template, report and original-rule rerun')
  # Reuse the established remote resource observation contract for failure probes.
  api.e2e.BASE=c.base
  def launch():
   code,job=c.call('/tasks',{'datasetId':data['id'],'rules':rules});assert code==200;api.started(job['id']);return job
  job=launch();nodes.pause()
  try:
   assert c.call('/tasks/'+job['id']+'/cancel',{})[0]==200
   deadline=time.monotonic()+25
   while time.monotonic()<deadline:
    state=c.call('/tasks/'+job['id'])[1];assert state['status']=='CANCELLING'
    if state['clusterExecution']['cleanupObservation']=='UNAVAILABLE_OR_INVALID_EVIDENCE':break
    time.sleep(.2)
   else:raise AssertionError('Missing unavailable cleanup observation')
   assert c.call('/tasks',{'datasetId':data['id'],'rules':rules})[0]==409;save(root/'cancel-pending.json',state)
  finally:nodes.resume()
  state=api.wait_task(job['id']);assert state['status']=='CANCELLED' and state['clusterExecution']['cleanupObservation']=='CLEAR' and nodes.empty();check('Standalone cancellation remains pending until remote cleanup can be observed')
  job=launch();original=c.call('/tasks/'+job['id'])[1]['execution'];nodes.pause()
  try:
   st.run(['docker','kill',project.service('backend')]);st.run(project.compose+['start','backend']);c=Client('http://'+st.run(project.compose+['port','backend','8080']).decode().strip()+'/api');ready(c);api.e2e.BASE=c.base
   state=c.call('/tasks/'+job['id'])[1];assert state['status']=='RUNNING' and state['execution']==original;assert c.call('/tasks',{'datasetId':data['id'],'rules':rules})[0]==409;save(root/'restart-pending.json',state)
  finally:nodes.resume()
  state=api.wait_task(job['id']);assert state['status']=='FAILED' and state['clusterExecution']['cleanupObservation']=='CLEAR' and nodes.empty();check('Driver/backend interruption and restart retain identity until remote cleanup is confirmed')
  st.run(project.compose+['stop','backend']);config.write_text(config.read_text().replace('      SPARK_DQ_EXECUTION: standalone','      SPARK_DQ_TASK_TIMEOUT_SECONDS: 5\n      SPARK_DQ_EXECUTION: standalone'));st.run(project.compose+['up','-d','--no-build','--pull','never']);c=Client('http://'+st.run(project.compose+['port','backend','8080']).decode().strip()+'/api');ready(c);api.e2e.BASE=c.base
  code,job=c.call('/tasks',{'datasetId':data['id'],'rules':rules});assert code==200;state=api.wait_task(job['id']);assert state['status']=='FAILED' and ('超过' in state['error'] or '超时' in state['error']);assert state['clusterExecution']['cleanupObservation']=='CLEAR' and nodes.empty();save(root/'budget-timeout.json',state);check('Standalone task budget timeout stops preparation/detection and confirms remote resources clear')
  st.run(project.compose+['stop','backend']);audit=st.audit(project);assert audit['ok'];save(root/'audit.json',audit);summary.update(ok=True,rows=len(rows),rules=len(rules),localTask=local['id'],remoteTask=remote['id'],replayTask=replay['id'],bothWorkersExecuted=True,timeoutScope='5-second combined preparation/detection budget; remote execution is proven separately')
 except Exception:
  (root/'failure.txt').write_text(traceback.format_exc());raise
 finally:
  if nodes is not None:
   with contextlib.suppress(Exception):nodes.resume()
  if project is not None:
   with contextlib.suppress(Exception):st.run(project.compose+['stop'],timeout=90)
  if nodes is not None:
   with contextlib.suppress(Exception):nodes.close()
  summary['cleanup']='Owned containers stopped; all volumes and networks retained';save(root/'summary.json',summary);print(json.dumps(dict(root=str(root),**summary)),flush=True)
if __name__=='__main__':main()
