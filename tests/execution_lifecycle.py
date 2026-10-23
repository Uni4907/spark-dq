"""Destructive fault injection ONLY into a freshly restored execution-* test clone."""
import argparse,contextlib,json,re,sys,time,uuid
from pathlib import Path
import e2e,execution_inputs as test
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'infrastructure'))
import storage as st,maintenance as m
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--project-json',type=Path,required=True);args=parser.parse_args()
 info=json.loads(args.project_json.read_text());folder=Path(info['folder']).resolve()
 assert folder.parent==ROOT/'.runtime' and folder.name.startswith('execution-')
 project=st.Project(Path(info['config']),Path(info['env']));cid=project.service('backend')
 assert cid!=st.Project().service('backend')
 label=st.run(['docker','inspect','--format','{{index .Config.Labels "com.docker.compose.project"}}',cid]).decode().strip();assert label.startswith('spark-dq-restore-')
 control=m.Control(project);control.wait();e2e.BASE=control.address();info['api']=e2e.BASE
 fixture=json.loads((folder/'api-verified.json').read_text());base=json.loads(Path(fixture['fixture']).read_text())
 request={'datasetId':base['datasetId'],'fields':['name'],'keys':['id'],'executionInput':{'format':'PARQUET','partitions':4}}
 def processes():return st.run(['docker','exec',cid,'ps','-eo','pid,args']).decode().splitlines()
 def process(task,main):
  end=time.monotonic()+45
  while time.monotonic()<end:
   for line in processes():
    if task in line and 'cn.sparkdq.engine.'+main+' ' in line:return line.split(None,1)[0]
   state=e2e.http('/tasks/'+task)[1]['status'];assert state in ('QUEUED','RUNNING'),state
   time.sleep(.03)
  raise AssertionError('Worker phase not observed')
 def signal(pid,task,kind):
  lines=[x for x in processes() if x.split(None,1)[0]==pid and task in x and 'cn.sparkdq.engine.' in x]
  if lines:st.run(['docker','exec','-u','0',cid,'kill','-'+kind,pid])
 def write(path,raw):st.run(['docker','exec','-i','-u','0',cid,'sh','-c','cat > "$1"','sh',path],data=raw)
 blocker=test.call('/tasks',request);pid=process(blocker['id'],'PreparedInput');signal(pid,blocker['id'],'STOP')
 try:
  queued=test.call('/tasks',request);assert queued['status']=='QUEUED'
  assert e2e.http('/tasks/'+queued['id']+'/cancel',b'')[0]==200
  cancelled_queue=test.wait(queued['id'],'CANCELLED');assert cancelled_queue['duration_ms']==0
  assert e2e.http('/tasks/'+blocker['id']+'/cancel',b'')[0]==200
  cancelled=test.wait(blocker['id'],'CANCELLED')
 finally:signal(pid,blocker['id'],'CONT')
 assert not any(blocker['id'] in line and 'cn.sparkdq.engine.' in line for line in processes())
 assert not any(r['result_id'] for task in (cancelled_queue,cancelled) for r in task['rules'])
 print('PASS real preparation process and queued copy cancellation; no successful results or live worker',flush=True)
 corrupt=test.call('/tasks',dict(request,executionInput={'format':'CSV','partitions':4}));pid=process(corrupt['id'],'Engine');signal(pid,corrupt['id'],'STOP')
 path=None;original=None
 try:
  running=e2e.http('/tasks/'+corrupt['id'])[1];assert len(running['snapshot']['input_manifest_sha256'])==64
  directory='/data/tasks/'+corrupt['id']+'/prepared/'
  manifest=json.loads(st.run(['docker','exec',cid,'cat',directory+'manifest.json']))
  relative=manifest['files'][0]['path'];assert re.fullmatch(r'data/part-[A-Za-z0-9_.-]+',relative)
  path=directory+relative;original=st.run(['docker','exec',cid,'cat',path]);write(path,original+b'X')
  signal(pid,corrupt['id'],'CONT')
  failed=test.wait(corrupt['id'],'FAILED');assert 'Prepared file size mismatch' in failed['error'],failed
  assert not any(rule['result_id'] for rule in failed['rules'])
 finally:
  if original is not None:
   write(path,original)
   assert st.run(['docker','exec',cid,'cat',path])==original,"Synthetic test part restore readback failed"
  signal(pid,corrupt['id'],'CONT')
 repaired=test.wait(test.call('/tasks/'+corrupt['id']+'/rerun',{})['id'])
 print('PASS bound copied part corruption persists FAILED without partial metrics; canonical CSV unchanged and rerun succeeds',flush=True)
 interrupted=test.call('/tasks',request);pid=process(interrupted['id'],'PreparedInput');signal(pid,interrupted['id'],'STOP')
 snapshot=e2e.http('/tasks/'+interrupted['id'])[1]['snapshot']
 st.run(['docker','kill','--signal','KILL',cid]);st.run(project.compose+['start','backend'],timeout=90)
 control.wait();e2e.BASE=control.address();info['api']=e2e.BASE
 recovered=test.wait(interrupted['id'],'FAILED')
 assert recovered['snapshot']==snapshot and '服务重启' in recovered['error'] and not any(r['result_id'] for r in recovered['rules'])
 rerun=test.wait(test.call('/tasks/'+recovered['id']+'/rerun',{})['id'])
 test.check(base)
 print('PASS real container crash during preparation -> persisted FAILED, immutable snapshot and independent successful rerun',flush=True)
 result={'ok':True,'fixtureRestoreReadbackVerified':True,'cancelledQueued':cancelled_queue,'cancelledPreparing':cancelled,'failedCorrupt':failed,
  'repairedTask':repaired,'failedRestart':recovered,'rerunRestart':rerun,'project':info}
 receipt=folder/('lifecycle-verified-'+uuid.uuid4().hex[:12]+'.json');st.save_json(receipt,result);(folder/'current-project.json').write_text(json.dumps(info))
 print(json.dumps({'ok':True,'output':str(receipt),'cases':6}),flush=True)
if __name__=='__main__':main()
