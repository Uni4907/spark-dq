#!/usr/bin/env python3
"""Real HTTP/Standalone acceptance on a stopped disposable restored project. Never uses primary volumes."""
import argparse,contextlib,json,os,re,shutil,signal,subprocess,sys,time,uuid
from pathlib import Path
import storage as st,benchmark as b,maintenance as m
sys.path.insert(0,str(st.ROOT/'tests'))
import e2e,native_imports_e2e as native,execution_inputs as x
BASE_IMAGE='apache/spark@sha256:8cd61978c71aafcd61ac37fcdbbca2ad774396407c9498c79a2afdfc57142783'
LABEL='sparkdq.cluster-api'
class Nodes:
 def __init__(self,root,network,files):self.root=root;self.network=network;self.files=files;self.session=uuid.uuid4().hex;self.items={};self.paused=set()
 def owned(self,cid):assert st.run(['docker','inspect','--format','{{index .Config.Labels "'+LABEL+'"}}',cid]).decode().strip()==self.session
 def start(self):
  for role in ('master','worker1','worker2'):
   folder=self.root/role;folder.mkdir();os.chown(folder,1000,1000)
   command=['/opt/spark/bin/spark-class','org.apache.spark.deploy.master.Master','--host','master','--port','7077','--webui-port','8080'] if role=='master' else ['/opt/spark/bin/spark-class','org.apache.spark.deploy.worker.Worker','--host',role,'--cores','1','--memory','1024m','--webui-port','8081','spark://master:7077']
   args=['docker','create','--name','sparkdq-'+self.session[:12]+'-'+role,'--label',LABEL+'='+self.session,'--network',self.network,'--network-alias',role,'--hostname',role,'--user','1000:1000','--workdir','/tmp','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','512','--memory','512m' if role=='master' else '2g','--cpus','.5' if role=='master' else '1','--tmpfs','/tmp:rw,exec,size=768m','--mount','type=bind,source='+str(folder)+',target=/trial','-e','SPARK_PUBLIC_DNS='+role,'-e','SPARK_DAEMON_MEMORY=256m','-e','SPARK_WORKER_DIR=/trial/work','-e','SPARK_LOG_DIR=/tmp/logs','-e','SPARK_LOCAL_DIRS=/tmp/spark','--entrypoint','/usr/bin/timeout']
   if role!='master':args+=['--mount','type=volume,source='+self.files+',target=/data']
   cid=st.run(args+['spark-dq-worker:cluster-api','--signal=TERM','--kill-after=10s','1800s',*command],timeout=45).decode().strip();self.items[role]=cid;st.save_json(self.root/(role+'-container.json'),{'id':cid,'session':self.session});st.run(['docker','start',cid])
 def pause(self):
  cid=self.items['master'];self.owned(cid);st.run(['docker','pause',cid],timeout=15);self.paused.add(cid)
 def resume(self):
  for cid in list(self.paused):self.owned(cid);st.run(['docker','unpause',cid],timeout=15);self.paused.remove(cid)
 def empty(self):return all('CoarseGrainedExecutorBackend' not in st.run(['docker','top',self.items[role],'-eo','pid,args']).decode() for role in ('worker1','worker2'))
 def close(self):
  self.resume()
  for role,cid in reversed(list(self.items.items())):
   self.owned(cid);st.run(['docker','stop','--time','5',cid],timeout=20)
   with (self.root/(role+'-final.log')).open('xb') as output:subprocess.run(['docker','logs',cid],stdout=output,stderr=subprocess.STDOUT,check=True,timeout=20)
def wait_task(identity,statuses=('SUCCEEDED','FAILED','CANCELLED'),timeout=330):
 end=time.monotonic()+timeout
 while time.monotonic()<end:
  code,node=e2e.http('/tasks/'+identity);assert code==200
  if node['status'] in statuses:return node
  time.sleep(.2)
 raise AssertionError('Task state deadline exceeded: '+identity)
def started(identity):
 end=time.monotonic()+60
 while time.monotonic()<end:
  node=e2e.http('/tasks/'+identity)[1]
  if node['status']=='RUNNING' and node['execution']['evidenceState']=='STARTED':assert node['execution']['started']['applicationId'].startswith('app-');return node
  assert node['status'] not in ('SUCCEEDED','FAILED','CANCELLED'),node
  time.sleep(.05)
 raise AssertionError('Remote application identity deadline exceeded')
def run(args):
 root=args.output.absolute();root.resolve().relative_to(st.ROOT/'.runtime');assert not root.exists();assert args.restore.resolve().is_relative_to(st.ROOT/'.runtime')
 receipt=json.loads((args.restore/'verified.json').read_text());assert receipt['state']=='verified' and receipt['project'].startswith('spark-dq-restore-')
 old=st.Project(args.restore/'compose.yaml',args.restore/'.env');assert not old.running('backend') and not old.running('mysql')
 assert st.Project().service('backend')!=old.service('backend')
 capacity=json.loads(args.host_capacity.read_text(encoding='utf-8-sig'));assert capacity.get('known') and capacity['freeBytes']>=8*1024**3 and shutil.disk_usage(st.ROOT).free>=8*1024**3
 before=b.source_snapshot();point=st.recovery_point(st.Project());root.mkdir(mode=0o700);st.save_json(root/'primary-before.json',before);st.save_json(root/'primary-point.json',point)
 images={s:st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(s)]).decode().strip() for s in ('backend','frontend')};st.save_json(root/'primary-images.json',images)
 network=receipt['project']+'_default';files=receipt['filesVolume'];assert files!="spark-dq_files" and files==receipt['project']+'-files'
 config=root/'candidate.yaml';text=(args.restore/'compose.yaml').read_text();backend=re.search(r'(?m)^    image: (sha256:[0-9a-f]+)$',text);assert backend
 text=text.replace('image: '+backend[1],'image: spark-dq-backend:cluster-api').replace('volumes: [files:/data, application:/app:ro]','volumes: [files:/data]')
 env='''      SPARK_DQ_EXECUTION: standalone
      SPARK_MASTER: spark://master:7077
      SPARK_DQ_DRIVER_HOST: driver
      SPARK_DQ_MASTER_JSON_URL: http://master:8080/json/
      SPARK_DQ_WORKER_JSON_URLS: http://worker1:8081/json/,http://worker2:8081/json/
      SPARK_INPUT_PARTITIONS: 8
      SPARK_SHUFFLE_PARTITIONS: 2
'''
 text=text.replace('      SPARK_LOCAL_IP: 127.0.0.1','      SPARK_LOCAL_IP: 127.0.0.1\n'+env.rstrip());text=text.replace('    ports: ["127.0.0.1::8080"]','    networks: {default: {aliases: [driver]}}\n    ports: ["127.0.0.1::8080"]');config.write_text(text)
 project=st.Project(config,args.restore/'.env');nodes=Nodes(root,network,files);control=m.Control(project)
 st.save_json(root/'project.json',{'folder':str(root),'config':str(config),'env':str(args.restore/'.env'),'project':receipt['project']})
 try:
  nodes.start();st.run(project.compose+['up','-d','--no-build','--pull','never'],timeout=100);control.wait();e2e.BASE=control.address()
  native_root=st.ROOT/'.runtime/native-parquet-154a4769c073'
  with (root/'api.log').open('x') as output:done=subprocess.run([sys.executable,'tests/native_imports_e2e.py','--base',e2e.BASE,'--source',str(native_root/'cases/typed/inputs/source.parquet'),'--expected',str(native_root/'expected.json')],cwd=st.ROOT,stdout=output,stderr=subprocess.STDOUT,timeout=900)
  assert done.returncode==0,(root/'api.log').read_text()[-6000:]
  fixture=json.loads(Path(json.loads((root/'api.log').read_text().splitlines()[-1])['fixture']).read_text())
  for task in fixture['tasks']:
   assert task['execution']['started']['applicationId'].startswith('app-') and task['clusterExecution']['cleanupObservation']=='CLEAR'
  print('PASS actual HTTP import/detection/prepared governance/recheck/report on remote Spark workers',flush=True)
  b.generate(root/'sample.csv',10000);code,data=e2e.upload((root/'sample.csv').read_bytes(),'cluster-api-synthetic.csv');assert code==200
  definitions=b.rules('extended');identity=x.call('/tasks',{'datasetId':data['id'],'rules':[{key:value for key,value in rule.items() if key!='id'} for rule in definitions]})['id'];checked=wait_task(identity);assert checked['status']=='SUCCEEDED' and checked['total_rows']==10000,checked
  expected=b.oracle(10000,'extended')['metrics']
  for definition in definitions:
   actual=next(rule for rule in checked['rules'] if rule['type']==definition['type'] and rule['columns']==definition['columns'] and rule.get('params',{})==definition.get('params',{}))
   assert actual['metrics']==expected[definition['id']]
  st.save_json(root/'large-api-metrics-verified.json',{'ok':True,'taskId':identity,'rows':10000,'rules':7,'independentOracle':True})
  proof=json.loads(st.run(['docker','exec',project.service('backend'),'cat','/data/tasks/'+identity+'/executor-evidence.json']));assert {row['host'] for row in proof['executors']}=={'worker1','worker2'} and all(row['successfulTasks']>0 for row in proof['executors'])
  st.save_json(root/'two-worker-proof.json',proof);print('PASS HTTP task has real successful tasks on both remote workers',flush=True)
  # An unreachable master must prevent terminal cancellation and subsequent launch.
  cancelled=e2e.create(data['id'],['name','email'],['id'])['id'];started(cancelled);nodes.pause()
  try:
   assert e2e.http('/tasks/'+cancelled+'/cancel',b'',method='POST')[0]==200
   end=time.monotonic()+25;code=None
   while time.monotonic()<end:
    node=e2e.http('/tasks/'+cancelled)[1];assert node['status']=='CANCELLING'
    if node['clusterExecution']['cleanupObservation']!='UNAVAILABLE_OR_INVALID_EVIDENCE':time.sleep(.2);continue
    code,_=e2e.http('/tasks',json.dumps({'datasetId':data['id'],'fields':['name'],'keys':['id']}).encode(),{'Content-Type':'application/json'})
    if code==409:break
    assert code==200;time.sleep(1)
   assert code==409,'Pending cleanup did not close admission'
   st.save_json(root/'unavailable-cancellation.json',{'taskId':cancelled,'status':node['status'],'newTaskStatus':code,'noTerminalOutcome':True})
  finally:nodes.resume()
  cancelled_task=wait_task(cancelled);assert cancelled_task['status']=='CANCELLED' and cancelled_task['clusterExecution']['cleanupObservation']=='CLEAR'
  assert nodes.empty();print('PASS unavailable-master cancellation stays pending, then releases remote resources and resumes',flush=True)
  # A hard client container restart must preserve the intent and delay recovery until all nodes can be checked.
  crash=e2e.create(data['id'],['name'],['id'])['id'];original=started(crash);nodes.pause()
  try:
   cid=project.service('backend');st.run(['docker','kill','--signal','KILL',cid],timeout=20);st.run(project.compose+['start','backend'],timeout=90);control.wait();e2e.BASE=control.address()
   recovered=e2e.http('/tasks/'+crash)[1];assert recovered['status']=='RUNNING' and recovered['execution']==original['execution']
   code,_=e2e.http('/tasks',json.dumps({'datasetId':data['id'],'fields':['name'],'keys':['id']}).encode(),{'Content-Type':'application/json'});assert code==409
   st.save_json(root/'unavailable-recovery.json',{'taskId':crash,'status':recovered['status'],'identityPreserved':True,'newTaskStatus':code})
  finally:nodes.resume()
  crashed=wait_task(crash);assert crashed['status']=='FAILED' and crashed['clusterExecution']['cleanupObservation']=='CLEAR';assert nodes.empty()
  # Some queued calls made before the cleanup gate closed may exist: let them drain.
  end=time.monotonic()+300
  while time.monotonic()<end:
   if st.run(['docker','exec',project.service('mysql'),'sh','-c','export MYSQL_PWD="$MYSQL_PASSWORD"; mysql -N -u sparkdq -D sparkdq -e "select count(*) from tasks where status in (\'QUEUED\',\'RUNNING\',\'CANCELLING\')"']).strip()==b'0':break
   time.sleep(.5)
  else:raise AssertionError('Isolated task queue did not drain')
  native.check(fixture);st.run(project.compose+['stop','backend'],timeout=60);audit=st.audit(project);assert audit['ok'];st.save_json(root/'audit.json',audit)
  summary={'ok':True,'remoteApiTasks':4,'rows':10000,'bothWorkersExecuted':True,'unavailableCancellationPending':True,'crashRecoveryPendingUntilObservedClear':True,'noRemoteSuccessInferred':True,'auditOk':True,'primaryUnchanged':True}
 finally:
  nodes.resume();st.run(project.compose+['stop'],timeout=90);nodes.close()
 assert before==b.source_snapshot() and point['sha256']==st.recovery_point(st.Project())['sha256']
 assert all(st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(service)]).decode().strip()==image for service,image in images.items())
 st.save_json(root/'verified.json',summary)
 print(json.dumps({'ok':True,'folder':str(root),'mainUnchanged':True,'allOwnedNodesStopped':True}),flush=True)
def main():
 def interrupted(signum,frame):raise KeyboardInterrupt('Interrupted; preserving evidence and stopping owned nodes')
 signal.signal(signal.SIGTERM,interrupted);p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--restore',type=Path,required=True);p.add_argument('--host-capacity',type=Path,required=True);run(p.parse_args())
if __name__=='__main__':main()
