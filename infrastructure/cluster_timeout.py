#!/usr/bin/env python3
"""Owned candidate-only real preparation/Engine timeout, cleanup gate and replay acceptance."""
import argparse,contextlib,hashlib,json,os,signal,sys,time
from pathlib import Path
import storage as st,benchmark as b,cluster_lab as lab,cluster_stability as soak,header_service_acceptance as service,standalone_trial as remote
sys.path.insert(0,str(st.ROOT/'tests'))
import e2e,execution_inputs as x
def process_identity(pid,container,main,request,parent,proc=Path('/proc')):
 folder=proc/str(pid);command=(folder/'cmdline').read_bytes().split(b'\0');fields=(folder/'stat').read_text().rsplit(')',1)[1].split()
 if main.encode() not in command or request.encode() not in command or int(fields[1])!=parent or container not in (folder/'cgroup').read_text():raise ValueError('Process is not the owned direct task child')
 return {'pid':pid,'startTicks':fields[19],'containerId':container,'main':main,'request':request,'parent':parent}
def same_process(identity,proc=Path('/proc')):
 try:return process_identity(identity['pid'],identity['containerId'],identity['main'],identity['request'],identity['parent'],proc)==identity
 except (OSError,ValueError,IndexError):return False
def assert_failure(task,seconds,phase):
 service.terminal(task,'FAILED');assert '准备与检测合计超过 '+str(seconds)+' 秒' in task['error'];assert task['total_rows'] is None and task['finished_at'] and task['duration_ms']>=seconds*1000
 assert all(row['metrics'] is None and row['result_id'] is None for row in task['rules'])
 if phase=='prepare':assert task['execution']['evidenceState']=='ABSENT' and task['snapshot']['input_manifest_sha256'] is None
 else:assert task['execution']['started']['applicationId'].startswith('app-') and task['snapshot']['input_manifest_sha256']
def run(args):
 output=args.output.absolute();output.resolve().relative_to(st.ROOT/'.runtime');assert not output.exists();control,node,project=lab.load(args.lab);lab.owned(control,node,project);state=lab.status(control);assert state['alive'] and state['state']=='running' and node['taskTimeoutSeconds']==60
 assert project.volume('backend','/data')!=st.Project().volume('backend','/data');e2e.BASE=state['api'];assert e2e.http('/datasets/page')[1]['total']==0
 ids={role:project.service(role) for role in soak.ROLES};raw=json.loads(st.run(['docker','inspect',ids['backend']]))[0];parent=raw['State']['Pid'];assert 'SPARK_DQ_TASK_TIMEOUT_SECONDS=60' in raw['Config']['Env']
 jar=st.run(['docker','exec',ids['backend'],'sha256sum','/app/engine/spark-engine-0.1.0.jar']).decode().split()[0];assert jar==args.engine_sha256
 output.mkdir(mode=0o700);(output/'trials').mkdir();before=b.source_snapshot();point=st.recovery_point(st.Project());st.save_json(output/'primary-before.json',before);st.save_json(output/'primary-point.json',point)
 images={role:st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(role)]).decode().strip() for role in ('backend','frontend')};st.save_json(output/'primary-images.json',images)
 sources={str(p.relative_to(st.ROOT)):st.digest(p) for p in (Path(__file__),st.ROOT/'infrastructure/cluster_lab.py',st.ROOT/'infrastructure/cluster_stability.py')};st.save_json(output/'environment.json',{'lab':str(control),'project':node['project'],'images':node['images'],'taskTimeoutSeconds':60,'engineSha256':jar,'sourceDigests':sources,'rows':1000})
 deadline=time.monotonic()+900;started=time.monotonic();own=[];stopped=[];paused=False;records=[];frozen={};reports={};idles=[];success=False
 def guard():
  if time.monotonic()>deadline:raise TimeoutError('Finite timeout acceptance deadline')
 def get(identity):
  code,task=e2e.http('/tasks/'+identity);assert code==200;return task
 def submit(path,body):
  guard();task=x.call(path,body);own.append(task['id']);return task['id']
 def wait(identity,wanted):
  end=time.monotonic()+110
  while time.monotonic()<end:
   guard();task=get(identity)
   if task['status'] in ('FAILED','CANCELLED','SUCCEEDED'):return service.terminal(task,wanted)
   time.sleep(.2)
  raise TimeoutError('Task terminal deadline')
 def stop_child(identity,phase):
  main='cn.sparkdq.engine.'+('PreparedInput' if phase=='prepare' else 'Engine');request='/data/tasks/'+identity+'/'+('prepare-request.json' if phase=='prepare' else 'request.json');end=time.monotonic()+40
  while time.monotonic()<end:
   guard();task=get(identity);assert task['status'] in ('QUEUED','RUNNING'),task
   if phase=='prepare' or task['execution']['evidenceState']=='STARTED':
    for line in st.run(['docker','top',ids['backend'],'-eo','pid,args']).decode().splitlines()[1:]:
     try:proof=process_identity(int(line.split()[0]),ids['backend'],main,request,parent)
     except (OSError,ValueError,IndexError):continue
     assert same_process(proof);os.kill(proof['pid'],signal.SIGSTOP);stopped.append(proof);proof=dict(proof,secondsFromRunStart=time.monotonic()-started);st.save_json(output/(phase+'-stopped-process.json'),proof);return task,proof
   time.sleep(.08)
  raise TimeoutError('Owned child launch deadline')
 def idle(label):
  assert e2e.http('/execution/status')[1]=={'mode':'standalone','cleanupPending':0,'acceptingTasks':True}
  for status in ('QUEUED','RUNNING','CANCELLING'):assert e2e.http('/tasks/page?pageSize=1&status='+status)[1]['total']==0
  for role in ('worker1','worker2'):assert 'CoarseGrainedExecutorBackend' not in st.run(['docker','top',ids[role],'-eo','pid,args']).decode()
  assert 'cn.sparkdq.engine.' not in st.run(['docker','top',ids['backend'],'-eo','pid,args']).decode()
  for role in soak.ROLES:
   value=json.loads(st.run(['docker','inspect',ids[role]]))[0];assert value['State']['Running'] and not value['State']['OOMKilled'] and value['RestartCount']==0
  item={'label':label,'seconds':time.monotonic()-started,'activeTasks':0,'executors':0};idles.append(item);st.save_json(output/('idle-'+str(len(idles))+'.json'),item)
 def capture(task,action):
  folder=output/'trials'/str(len(records)+1);folder.mkdir();st.save_json(folder/'task.json',task);st.run(['docker','cp',ids['backend']+':/data/tasks/'+task['id'],str(folder/'result')],timeout=60)
  code,report=e2e.http('/tasks/'+task['id']+'/report');assert code==200
  if task['status']=='FAILED':assert 'outputDataset' not in report and 'comparison' not in report and all(row['metrics'] is None for row in report['task']['rules'])
  else:
   expected=b.oracle(1000,'extended');soak.validate_full(task,folder/'result',expected,soak.expected_anomalies(1000,'extended'));remote.evidence(folder/'result/executor-evidence.json',{'worker1','worker2'},2)
   assert task['execution']['evidenceState']=='DRIVER_ENDED' and json.loads((folder/'result/prepared/preparation.json').read_text())['verified']
  st.save_json(folder/'report.json',report);reports[task['id']]=report;frozen[task['id']]=task;record={'id':task['id'],'action':action,'status':task['status'],'durationMs':task['duration_ms'],'executionEvidence':task['execution']['evidenceState']};records.append(record);st.save_json(folder/'record.json',record);print(json.dumps(record),flush=True)
 try:
  idle('initial');b.generate(output/'input.csv',1000);body=(output/'input.csv').read_bytes();code,dataset=e2e.upload(body,'timeout-synthetic.csv');assert code==200
  request={'datasetId':dataset['id'],'rules':[{key:value for key,value in rule.items() if key!='id'} for rule in b.rules('extended')],'executionInput':{'format':'CSV','partitions':4}}
  capture(wait(submit('/tasks',request),'SUCCEEDED'),'baseline');idle('baseline')
  preparing=submit('/tasks',request);stop_child(preparing,'prepare');failed=wait(preparing,'FAILED');assert_failure(failed,60,'prepare');capture(failed,'prepare-timeout');idle('prepare-timeout')
  replay=wait(submit('/tasks/'+preparing+'/rerun',{}),'SUCCEEDED');assert replay['snapshot']['parent_task_id']==preparing and replay['dataset_id']==dataset['id'];capture(replay,'prepare-replay');idle('prepare-replay')
  executing=submit('/tasks',request);begun,proof=stop_child(executing,'engine');st.save_json(output/'engine-before-timeout.json',begun)
  lab.owned(control,node,project);assert st.run(['docker','inspect','--format','{{.State.Paused}}',ids['master']]).strip()==b'false';st.run(['docker','pause',ids['master']],timeout=15);paused=True;pause_start=time.monotonic()
  try:
   end=time.monotonic()+75
   while time.monotonic()<end:
    guard();task=get(executing);assert task['status']=='RUNNING',task
    if task.get('error') and '准备与检测合计超过 60 秒' in task['error']:break
    time.sleep(.2)
   else:raise TimeoutError('Real Engine budget expiry not observed')
   held=[]
   for i in range(4):
    task=get(executing);gate=e2e.http('/execution/status')[1];assert task['status']=='RUNNING' and all(v['metrics'] is None for v in task['rules']) and not gate['acceptingTasks'] and gate['cleanupPending']>=1
    assert e2e.http('/tasks/'+executing+'/report')[0]==409
    code,rejected=e2e.http('/tasks',json.dumps(request).encode(),{'Content-Type':'application/json'});assert code==409
    held.append({'task':task,'gate':gate,'rejectedStatus':code});time.sleep(.6)
   assert time.monotonic()-pause_start<90;st.save_json(output/'unavailable-cleanup-gate.json',{'observations':held,'masterPausedSeconds':time.monotonic()-pause_start})
  finally:
   st.run(['docker','unpause',ids['master']],timeout=15);paused=False
  failed=wait(executing,'FAILED');assert_failure(failed,60,'engine');assert failed['execution']['started']['applicationId']==begun['execution']['started']['applicationId'];capture(failed,'engine-timeout');idle('engine-timeout')
  prep=json.loads(st.run(['docker','exec',ids['backend'],'cat','/data/tasks/'+executing+'/prepared/preparation.json']));assert prep['verified'] and prep['preparationMs']>0
  # If each process received a fresh 60s budget, Engine timeout would include preparation on top.
  expiry_ms=held[0]['task']['duration_ms'];assert expiry_ms<60000+prep['preparationMs'],'Observed task deadline was reset after preparation'
  st.save_json(output/'shared-budget.json',{'persistedBudgetExpiryMs':expiry_ms,'terminalDurationMs':failed['duration_ms'],'preparationMs':prep['preparationMs'],'budgetMs':60000,'separateProcessBudgetsRejectedByObservation':True})
  replay=wait(submit('/tasks/'+executing+'/rerun',{}),'SUCCEEDED');assert replay['snapshot']['parent_task_id']==executing and replay['dataset_id']==dataset['id'];capture(replay,'engine-replay');idle('engine-replay')
  # Same candidate volumes and same 60s budget: terminal failures survive a real backend restart.
  st.run(project.compose+['stop','backend'],timeout=40);st.run(project.compose+['start','backend'],timeout=40);e2e.BASE=lab.address(project,'backend',8080)+'/api';end=time.monotonic()+100
  while time.monotonic()<end:
   try:
    if e2e.http('/health')[1]['status']=='UP':break
   except (OSError,ValueError):pass
   time.sleep(.4)
  else:raise TimeoutError('Restart health deadline')
  for identity,expected in frozen.items():assert get(identity)==expected and e2e.http('/tasks/'+identity+'/report')[1]==reports[identity]
  assert x.download(dataset['id'])==body and e2e.http('/tasks/page?pageSize=1')[1]['total']==5;idle('after-restart');success=True
 finally:
  if paused:st.run(['docker','unpause',ids['master']],timeout=15)
  for proof in stopped:
   if same_process(proof):os.kill(proof['pid'],signal.SIGCONT)
  if not success:
   for identity in own:
    with contextlib.suppress(OSError,ValueError):
     if get(identity)['status'] in ('QUEUED','RUNNING','CANCELLING'):e2e.http('/tasks/'+identity+'/cancel',b'',method='POST')
 assert before==b.source_snapshot() and point['sha256']==st.recovery_point(st.Project())['sha256'];assert all(st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(role)]).decode().strip()==value for role,value in images.items());assert all(st.digest(st.ROOT/name)==value for name,value in sources.items())
 summary={'ok':True,'elapsedSeconds':time.monotonic()-started,'taskBudgetSeconds':60,'successfulTasks':3,'expectedTimeoutFailures':2,'blockedSubmissions':4,'idleChecks':len(idles),'prepareTimeoutHasNoEngineReceiptOrResults':True,'engineTimeoutPreservesApplicationIdentity':True,'unavailableCleanupHeldNonterminalAndBlockedAdmission':True,'sharedPreparationEngineBudgetObserved':True,'bothReplaysVerifiedOnBothWorkers':True,'restartPreservesTerminalTasksReportsAndInput':True,'primaryDataFilesImagesUnchanged':True,'scope':'isolated same-host 60s test budget with exact owned child SIGSTOP and bounded Master pause; main 300s budget unchanged; not CPU saturation, cross-host or production HA proof'}
 st.save_json(output/'summary.json',summary);st.save_json(output/'records.json',records);st.save_json(output/'COMPLETE',{'summary':st.digest(output/'summary.json'),'records':st.digest(output/'records.json'),'gate':st.digest(output/'unavailable-cleanup-gate.json'),'budget':st.digest(output/'shared-budget.json')});print(json.dumps(summary),flush=True)
def main():
 signal.signal(signal.SIGTERM,lambda *unused:(_ for _ in ()).throw(InterruptedError('Finite timeout acceptance interrupted')))
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--lab',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--engine-sha256',required=True);run(parser.parse_args())
if __name__=='__main__':main()
