#!/usr/bin/env python3
"""Isolated same-host Standalone acceptance, not the production HTTP scheduler."""
import argparse,collections,contextlib,csv,hashlib,json,os,re,shutil,subprocess,sys,time,uuid,signal
from pathlib import Path
import storage as st,benchmark as b,runtime as rt

IMAGE='apache/spark@sha256:8cd61978c71aafcd61ac37fcdbbca2ad774396407c9498c79a2afdfc57142783'
LABEL='sparkdq.standalone'

def evidence(path,worker_hosts=None,minimum_workers=2):
 node=json.loads(path.read_text());assert node.get('version')==1 and node.get('applicationEnded') is True and node.get('truncated') is False,node
 rows=node.get('executors');assert isinstance(rows,list) and 1<=len(rows)<=64
 assert all(type(row.get('successfulTasks')) is int and row['successfulTasks']>0 and type(row.get('failedTasks')) is int and row['failedTasks']==0 for row in rows),rows
 if worker_hosts is not None:
  assert minimum_workers<=len(rows)<=len(worker_hosts) and len({row['id'] for row in rows})==len(rows) and {row['host'] for row in rows}.issubset(set(worker_hosts)) and all(row['id']!='driver' for row in rows),rows
 return node

def validate(folder,expected,master,worker_hosts=None,minimum_workers=2):
 result=json.loads((folder/'result/result.json').read_text());assert result['sparkVersion']=='3.5.8' and result['totalRows']==expected['totalRows']
 assert {row['ruleId']:row['metrics'] for row in result['results']}==expected['metrics']
 anomalies={}
 for rule in result['results']:
  rows=[]
  for part in (folder/'result/anomalies'/rule['ruleId']).glob('part-*.json'):
   rows.extend(json.loads(line) for line in part.read_text().splitlines())
  metrics=rule['metrics'];assert len(rows)==metrics.get('blankCount',metrics.get('duplicateRows',metrics.get('invalidCount')))
  assert all(set(row)==set(b.COLUMNS)|({'__dq_group_size'} if rule['ruleId']=='duplicate-id' else set()) for row in rows)
  anomalies[rule['ruleId']]=b.row_digest([[row.get(name) for name in b.COLUMNS]+([row['__dq_group_size']] if '__dq_group_size' in row else []) for row in rows])
 if 'audit' in expected:
  assert result['governance']==expected['audit'];parts=list((folder/'result/governed').glob('part-*.csv'));assert len(parts)==1
  with parts[0].open(newline='',encoding='utf-8-sig') as stream:
   reader=csv.reader(stream);assert next(reader)==b.COLUMNS;assert b.row_digest(reader)==expected['outputRowsSha256']
 assert result['execution']['sparkMaster']==master and result['execution']['inputPartitions']==8 and result['execution']['shufflePartitions']==2
 proof=evidence(folder/'executor-evidence.json',worker_hosts,minimum_workers)
 return {'metrics':{row['ruleId']:row['metrics'] for row in result['results']},'anomalySha256':anomalies,'executorEvidence':proof,'execution':result['execution']}

class Cluster:
 def __init__(self,root,session):self.root=root;self.session=session;self.network='sparkdq-standalone-'+session[:12];self.containers=[];self.workers=[];self.network_created=False
 def owned(self,cid):
  assert st.run(['docker','inspect','--format','{{index .Config.Labels "'+LABEL+'"}}',cid]).decode().strip()==self.session,'Container ownership mismatch'
 def create(self,role,command,*,network=True,cpus=1,memory='2g',extra=()):
  args=['docker','create','--name','sparkdq-'+self.session[:12]+'-'+role,'--label',LABEL+'='+self.session,'--hostname',role,'--user','185:0','--workdir','/tmp','--read-only','--cpus',str(cpus),'--memory',memory,'--memory-swap',memory,'--pids-limit','512','--cap-drop','ALL','--security-opt','no-new-privileges','--tmpfs','/tmp:rw,exec,size=768m',
   '--mount','type=bind,src='+str(self.root/'inputs')+',dst=/cluster/input,readonly','--mount','type=bind,src='+str(self.root/'output')+',dst=/cluster/output','--mount','type=bind,src='+str(self.root/'app.jar')+',dst=/cluster/app.jar,readonly',
   '-e','SPARK_DAEMON_MEMORY=256m','-e','SPARK_WORKER_DIR=/cluster/output/'+role+'-runtime','-e','SPARK_LOG_DIR=/tmp/logs','-e','SPARK_LOCAL_DIRS=/tmp/spark']
  args+=['--network',self.network,'--network-alias',role] if network else ['--network','none']
  args+=list(extra)+['--entrypoint','/usr/bin/timeout',IMAGE,'--signal=TERM','--kill-after=10s','1800s',*command]
  cid=st.run(args,timeout=60).decode().strip();self.containers.append((role,cid));st.save_json(self.root/(role+'-container.json'),{'role':role,'containerId':cid,'session':self.session});return cid
 def logs(self,cid,path):
  with path.open('xb') as out:subprocess.run(['docker','logs',cid],stdout=out,stderr=subprocess.STDOUT,timeout=30,check=True)
 def start_cluster(self):
  st.run(['docker','network','create','--internal','--label',LABEL+'='+self.session,self.network],timeout=30);self.network_created=True
  master=self.create('master',['/opt/spark/bin/spark-class','org.apache.spark.deploy.master.Master','--host','master','--port','7077','--webui-port','8080'],cpus=.5,memory='512m');st.run(['docker','start',master])
  for role in ('worker1','worker2'):
   cid=self.create(role,['/opt/spark/bin/spark-class','org.apache.spark.deploy.worker.Worker','--host',role,'--cores','1','--memory','1024m','--webui-port','8081','spark://master:7077']);self.workers.append(cid);st.run(['docker','start',cid])
  addresses={role:st.run(['docker','inspect','--format','{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}',cid]).decode().strip() for role,cid in zip(('worker1','worker2'),self.workers)}
  assert len(set(addresses.values()))==2 and all(re.fullmatch(r'[0-9.]+',host) for host in addresses.values())
  self.hosts=set(addresses)
  st.save_json(self.root/'worker-addresses.json',addresses)
 def folder(self,name):
  folder=self.root/'output'/name;folder.mkdir();os.chown(folder,185,0);return folder
 def job(self,name,payload,*,cluster=True,main='Engine',async_start=False):
  folder=self.folder(name);st.save_json(folder/'request.json',payload);os.chown(folder/'request.json',185,0)
  conf={'spark.ui.enabled':'false','spark.sql.adaptive.enabled':'false','spark.sql.shuffle.partitions':'2','spark.executor.memory':'768m','spark.executor.cores':'1','spark.cores.max':'2','spark.dynamicAllocation.enabled':'false','spark.scheduler.minRegisteredResourcesRatio':'1','spark.scheduler.maxRegisteredResourcesWaitingTime':'30s'}
  if main=='Engine':conf['spark.extraListeners']='cn.sparkdq.engine.ClusterEvidence'
  master='spark://master:7077' if cluster else 'local[2]';command=['/opt/spark/bin/spark-submit','--master',master,'--deploy-mode','client','--driver-memory','1g']
  for key,value in conf.items():command+=['--conf',key+'='+value]
  command+=['--class','cn.sparkdq.engine.'+main,'/cluster/app.jar','/cluster/output/'+name+'/request.json']
  env=['-e','SPARK_MASTER='+master,'-e','SPARK_INPUT_PARTITIONS=8','-e','SPARK_SHUFFLE_PARTITIONS=2','-e','SPARK_DQ_EVIDENCE=/cluster/output/'+name+'/executor-evidence.json']
  if cluster:env+=['-e','SPARK_DQ_DRIVER_HOST=driver','--network-alias','driver']
  else:env+=['-e','SPARK_LOCAL_IP=127.0.0.1']
  cid=self.create(name,command,network=cluster,cpus=2,extra=env);start=time.monotonic()
  if async_start:st.run(['docker','start',cid]);return cid,folder
  try:
   st.run(['docker','start',cid]);end=time.monotonic()+300
   while time.monotonic()<end:
    state=json.loads(st.run(['docker','inspect','--format','{{json .State}}',cid]))
    if not state['Running']:break
    time.sleep(.5)
   else:raise st.StorageError('Cluster job exceeded 300 seconds')
   assert not state['OOMKilled'];self.logs(cid,folder/'driver.log');return cid,folder,state['ExitCode'],round(time.monotonic()-start,3)
  finally:
   self.owned(cid)
   if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':st.run(['docker','stop','--time','5',cid],timeout=20)
 def no_executors(self):
  return all('CoarseGrainedExecutorBackend' not in st.run(['docker','top',cid,'-eo','pid,args']).decode() for cid in self.workers)
 def wait_empty(self):
  end=time.monotonic()+45
  while time.monotonic()<end:
   if self.no_executors():return
   time.sleep(.5)
  raise st.StorageError('Owned worker executors did not exit')
 def cancel(self,payload):
  cid,folder=self.job('cancelled',payload,async_start=True);end=time.monotonic()+60
  while time.monotonic()<end:
   if all('CoarseGrainedExecutorBackend' in st.run(['docker','top',worker,'-eo','pid,args']).decode() for worker in self.workers):break
   if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()!=b'true':raise st.StorageError('Cancellation probe ended before executors started')
   time.sleep(.1)
  else:raise st.StorageError('Cancellation probe did not allocate both executors')
  self.owned(cid);st.run(['docker','stop','--time','5',cid],timeout=20);self.wait_empty();self.logs(cid,folder/'driver.log');assert not (folder/'result/result.json').exists()
  return {'driverContainer':cid,'bothExecutorsAllocated':True,'executorsExited':True,'resultPublished':False}
 def close(self):
  for role,cid in reversed(self.containers):
   self.owned(cid)
   if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':st.run(['docker','stop','--time','5',cid],timeout=20)
   path=self.root/(role+'-final.log')
   if not path.exists():self.logs(cid,path)
  if self.network_created:
   info=json.loads(st.run(['docker','network','inspect',self.network]))[0]
   assert info['Labels'].get(LABEL)==self.session and not info['Containers'],'Network still used or ownership mismatch; no removal'
   st.run(['docker','network','rm',self.network],timeout=30);self.network_created=False

def run(args):
 root=args.output.absolute();root.resolve().relative_to(st.ROOT/'.runtime');assert not root.exists() and root.parent.is_dir()
 capacity=json.loads(args.host_capacity.read_text(encoding='utf-8-sig'));assert capacity.get('known') and capacity['freeBytes']>=8*1024**3 and shutil.disk_usage(root.parent).free>=8*1024**3
 assert 100<=args.rows<=20000 and rt.active(st.Project())==0
 before=b.source_snapshot();point=st.recovery_point(st.Project());root.mkdir(mode=0o700);st.save_json(root/'primary-before.json',before);st.save_json(root/'primary-point.json',point)
 images={s:st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(s)]).decode().strip() for s in ('backend','frontend')};st.save_json(root/'primary-images.json',images)
 (root/'inputs').mkdir(mode=0o755);(root/'output').mkdir(mode=0o755);os.chown(root/'output',185,0);shutil.copyfile(st.ROOT/'spark-engine/target/spark-engine-0.1.0.jar',root/'app.jar');os.chmod(root/'app.jar',0o444)
 data=b.generate(root/'inputs/data.csv',args.rows);session=uuid.uuid4().hex;cluster=Cluster(root,session)
 st.save_json(root/'environment.json',{'image':IMAGE,'session':session,'sourceJar':st.digest(root/'app.jar'),'input':data,'hostCapacity':capacity,'topology':'single-host Docker internal network; 1 master, 2 workers, client driver; shared bind paths; no published ports'})
 def payload(name,profile='extended',prepared=False):
  p={'taskId':name,'output':'/cluster/output/'+name+'/result','semanticsVersion':'csv-v2','rules':b.rules(profile)}
  if prepared:p.update(inputManifest='/cluster/input/prepared/manifest.json',inputManifestSha256=st.digest(root/'inputs/prepared/manifest.json')['sha256'])
  else:p['input']='/cluster/input/data.csv'
  if profile=='governance':p['governance']=b.policy()
  return p
 try:
  _,folder,code,wall=cluster.job('local',payload('local'),cluster=False);assert code==0,(code,str(folder));baseline=validate(folder,b.oracle(args.rows,'extended'),'local[2]');st.save_json(root/'local-verified.json',dict(ok=True,wallSeconds=wall,**baseline));print('PASS local baseline against independent expected values',flush=True)
  prepare={'input':'/cluster/input/data.csv','output':'/cluster/output/prepare/prepared','format':'PARQUET','partitions':4}
  _,prepared,code,wall=cluster.job('prepare',prepare,cluster=False,main='PreparedInput');assert code==0
  shutil.copytree(prepared/'prepared',root/'inputs/prepared')
  for path in (root/'inputs/prepared').rglob('*'):
   if path.is_file():os.chmod(path,0o444)
  cluster.start_cluster()
  _,folder,code,wall=cluster.job('distributed',payload('distributed',prepared=True));assert code==0,(code,str(folder));distributed=validate(folder,b.oracle(args.rows,'extended'),'spark://master:7077',cluster.hosts)
  assert distributed['metrics']==baseline['metrics'] and distributed['anomalySha256']==baseline['anomalySha256'];st.save_json(root/'distributed-verified.json',dict(ok=True,wallSeconds=wall,**distributed));print('PASS both remote executor hosts completed real tasks; metrics and full anomalies match local oracle',flush=True);cluster.wait_empty()
  _,folder,code,wall=cluster.job('governed',payload('governed','governance',True));assert code==0,(code,str(folder));governed=validate(folder,b.oracle(args.rows,'governance'),'spark://master:7077',cluster.hosts);st.save_json(root/'governance-verified.json',dict(ok=True,wallSeconds=wall,**governed));print('PASS remote governance, operation audit and exact independent governed row multiset',flush=True);cluster.wait_empty()
  cancelled=cluster.cancel(payload('cancelled',prepared=True));st.save_json(root/'cancel-verified.json',dict(ok=True,**cancelled));print('PASS cancelled driver released both worker executors without publishing result',flush=True)
  replay=payload('recheck');governed_file=next((root/'output/governed/result/governed').glob('part-*.csv'));replay['input']='/cluster/output/governed/result/governed/'+governed_file.name
  _,folder,code,wall=cluster.job('recheck',replay);assert code==0,(code,str(folder));expected=b.oracle(args.rows,'governance');expected.pop('audit');expected.pop('outputRowsSha256');checked=validate(folder,expected,'spark://master:7077',cluster.hosts,minimum_workers=1);assert checked['metrics']==governed['metrics'] and checked['anomalySha256']==governed['anomalySha256'];st.save_json(root/'recheck-verified.json',dict(ok=True,wallSeconds=wall,**checked));cluster.wait_empty();print('PASS cluster accepts next job after cancellation; governance CSV recheck preserves metrics/anomalies',flush=True)
  bad=payload('bad-manifest',prepared=True);bad['inputManifestSha256']='0'*64
  _,folder,code,wall=cluster.job('bad-manifest',bad);assert code!=0 and not (folder/'result/result.json').exists();cluster.wait_empty();st.save_json(root/'failure-verified.json',{'ok':True,'exitCode':code,'badManifestRejected':True,'resultPublished':False,'executorsExited':True});print('PASS bad frozen manifest refused and cluster executors cleaned',flush=True)
 finally:cluster.close()
 assert before==b.source_snapshot() and st.recovery_point(st.Project())['sha256']==point['sha256']
 assert all(st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(s)]).decode().strip()==image for s,image in images.items())
 assert st.digest(root/'inputs/data.csv')=={'sha256':data['sha256'],'size':data['size']}
 summary={'ok':True,'rows':args.rows,'workers':2,'workerHosts':sorted(cluster.hosts),'successCases':['local','distributed','governed','recheck'],'failureCases':['cancelled-driver','bad-manifest'],'mainUnchanged':True,'allOwnedContainersStopped':True,'ownedEmptyNetworkRemoved':not cluster.network_created,'perJobTaskHosts':{name:sorted(row['host'] for row in json.loads((root/'output'/name/'executor-evidence.json').read_text())['executors']) for name in ('distributed','governed','recheck')},'performanceClaim':False,'image':IMAGE,'session':session}
 st.save_json(root/'verified.json',summary);print(json.dumps(summary),flush=True)

def main():
 def interrupted(signum,frame):raise KeyboardInterrupt('Interrupted; owned containers will be stopped')
 signal.signal(signal.SIGTERM,interrupted)
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--host-capacity',type=Path,required=True);p.add_argument('--rows',type=int,default=10000);a=p.parse_args();run(a)
if __name__=='__main__':main()
