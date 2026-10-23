#!/usr/bin/env python3
"""Bounded paired local/Standalone comparison, synthetic data only, no HTTP writes."""
import argparse,csv,fcntl,ipaddress,json,os,random,shutil,signal,statistics,subprocess,time,uuid
from pathlib import Path
import benchmark as b,storage as st,standalone_trial as s

def plan(rows,repeats,seed):
 if not rows or len(set(rows))!=len(rows) or any(type(n) is not int or not 100<=n<=250000 for n in rows) or repeats not in (1,2,3):raise ValueError('Distinct rows 100..250000 and 1..3 repeats required')
 pairs=[dict(rows=n,profile=p,repeat=i,warmup=False) for n in rows for p in ('extended','governance') for i in range(1,repeats+1)];rng=random.Random(seed);rng.shuffle(pairs)
 trials=[dict(rows=min(rows),profile='extended',repeat=0,warmup=True,mode=mode) for mode in ('local','cluster')]
 for pair in pairs:
  modes=['local','cluster'];rng.shuffle(modes);trials.extend(dict(pair,mode=mode) for mode in modes)
 if len(trials)>40:raise ValueError('At most 40 trials')
 return [dict(t,index=i) for i,t in enumerate(trials,1)]

def read_cgroup(path):
 try:
  cpu=dict(line.split() for line in (path/'cpu.stat').read_text().splitlines());memory=int((path/'memory.current').read_text())
  return {'cpuUsec':int(cpu['usage_usec']),'memoryBytes':memory}
 except FileNotFoundError:return None

def resources(samples,baseline,roles):
 if not samples or any(not any(role in sample['roles'] for sample in samples) for role in roles):raise ValueError('Missing resource samples')
 last={};peak=0
 for sample in samples:
  peak=max(peak,sum(value['memoryBytes'] for value in sample['roles'].values()))
  for role,value in sample['roles'].items():
   if value['cpuUsec']<last.get(role,baseline.get(role,0)):raise ValueError('CPU counter reset')
   last[role]=value['cpuUsec']
 return {'sampledCpuSecondsLowerBound':sum(last[role]-baseline.get(role,0) for role in roles)/1e6,'sampledAggregateMemoryPeakBytes':peak,'samples':len(samples)}

def cgroup(cid):
 pid=int(st.run(['docker','inspect','--format','{{.State.Pid}}',cid]).strip());entries=(Path('/proc')/str(pid)/'cgroup').read_text().splitlines();rel=next(line.split(':',2)[2] for line in entries if line.startswith('0::'))
 path=(Path('/sys/fs/cgroup')/rel.lstrip('/')).resolve();path.relative_to('/sys/fs/cgroup');assert cid in str(path),'Cgroup identity mismatch';assert (path/'cpu.stat').is_file();return path

def choose_subnet(used):
 networks=[ipaddress.ip_network(value,strict=False) for value in used if value not in ('default','0.0.0.0/0','::/0')]
 for block in ('10.240.240.0/24','192.168.240.0/24','10.241.240.0/24',*[f'10.{n}.240.0/24' for n in range(242,250)]):
  for candidate in ipaddress.ip_network(block).subnets(new_prefix=28):
   if not any(other.version==4 and candidate.overlaps(other) for other in networks):return str(candidate)
 raise ValueError('No nonoverlapping bounded lab subnet available')

class Comparison(s.Cluster):
 def start_cluster(self):
  ids=st.run(['docker','network','ls','-q']).decode().split();nodes=json.loads(st.run(['docker','network','inspect',*ids]));used=[cfg['Subnet'] for node in nodes for cfg in (node.get('IPAM',{}).get('Config') or []) if cfg.get('Subnet')]
  used.extend(route['dst'] for route in json.loads(st.run(['ip','-j','route','show','table','all'])) if route.get('dst'))
  subnet=choose_subnet(used);st.save_json(self.root/'network-plan.json',{'subnet':subnet,'existingSubnets':used,'internal':True})
  st.run(['docker','network','create','--internal','--subnet',subnet,'--label',s.LABEL+'='+self.session,self.network],timeout=30);self.network_created=True
  master=self.create('master',['/opt/spark/bin/spark-class','org.apache.spark.deploy.master.Master','--host','master','--port','7077','--webui-port','8080']);st.run(['docker','start',master])
  for role in ('worker1','worker2'):
   cid=self.create(role,['/opt/spark/bin/spark-class','org.apache.spark.deploy.worker.Worker','--host',role,'--cores','1','--memory','1024m','--webui-port','8081','spark://master:7077']);self.workers.append(cid);st.run(['docker','start',cid])
  self.hosts={'worker1','worker2'}

 def create(self,role,command,**options):
  if role=='master':cpu,memory=.5,'512m'
  elif role.startswith('worker'):cpu,memory=.75,'1536m'
  elif role.startswith('local-'):cpu,memory=2,'5g'
  else:cpu,memory=.5,'2g'
  options.update(cpus=cpu,memory=memory);return super().create(role,command,**options)
 def input_request(self,trial):return {'input':'/cluster/input/'+str(trial['rows'])+'.csv'}
 def check_input(self,trial,execution):pass
 def idle(self):b.source_idle()
 def measure(self,trial,deadline):
  mode=trial['mode'];name=mode+'-'+str(trial['index']);folder=self.folder(name);remote=mode=='cluster'
  payload={'taskId':name,'output':'/cluster/output/'+name+'/result','semanticsVersion':'csv-v2','rules':b.rules(trial['profile'])}
  payload.update(self.input_request(trial))
  if trial['profile']=='governance':payload['governance']=b.policy()
  st.save_json(folder/'request.json',payload);os.chown(folder/'request.json',185,0)
  master='spark://master:7077' if remote else 'local[2]'
  command=['/opt/spark/bin/spark-submit','--master',master,'--deploy-mode','client','--driver-memory','1g' if remote else '2560m']
  conf={'spark.locality.wait':'0s','spark.ui.enabled':'false','spark.sql.adaptive.enabled':'false','spark.sql.shuffle.partitions':'2','spark.executor.memory':'768m','spark.executor.cores':'1','spark.cores.max':'2','spark.dynamicAllocation.enabled':'false','spark.scheduler.minRegisteredResourcesRatio':'1','spark.scheduler.maxRegisteredResourcesWaitingTime':'30s','spark.extraListeners':'cn.sparkdq.engine.ClusterEvidence'}
  for key,value in conf.items():command+=['--conf',key+'='+value]
  command+=['--class','cn.sparkdq.engine.Engine','/cluster/app.jar','/cluster/output/'+name+'/request.json']
  env=['-e','SPARK_MASTER='+master,'-e','SPARK_INPUT_PARTITIONS=8','-e','SPARK_SHUFFLE_PARTITIONS=2','-e','SPARK_DQ_EVIDENCE=/cluster/output/'+name+'/executor-evidence.json']
  env+=['-e','SPARK_DQ_DRIVER_HOST=driver','--network-alias','driver'] if remote else ['-e','SPARK_LOCAL_IP=127.0.0.1','--add-host',name+':127.0.0.1']
  cid=self.create(name,command,network=remote,extra=env)
  targets={role:cgroup(value) for role,value in self.containers if remote and role in ('master','worker1','worker2')};baseline={role:read_cgroup(path)['cpuUsec'] for role,path in targets.items()};samples=[];started=time.monotonic();waiter=None
  try:
   st.run(['docker','start',cid],timeout=20);targets['driver']=cgroup(cid);waiter=subprocess.Popen(['docker','wait',cid],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
   while True:
    sample={'seconds':time.monotonic()-started,'roles':{role:value for role,path in targets.items() if (value:=read_cgroup(path)) is not None}};samples.append(sample)
    if waiter.poll() is not None:break
    if time.monotonic()-started>300 or time.monotonic()>deadline:raise st.StorageError('Bounded trial/group deadline exceeded')
    self.idle() if len(samples)%40==0 else None
    time.sleep(.25)
   stdout,stderr=waiter.communicate(timeout=2);wall=time.monotonic()-started;assert waiter.returncode==0 and stdout.strip()==b'0',(stdout,stderr)
   state=json.loads(st.run(['docker','inspect','--format','{{json .State}}',cid]));assert not state['OOMKilled'] and state['ExitCode']==0
   self.logs(cid,folder/'driver.log');value=s.validate(folder,b.oracle(trial['rows'],trial['profile'],self.seed),master,self.hosts if remote else None)
   result=json.loads((folder/'result/result.json').read_text());self.check_input(trial,result['execution']);summary=resources(samples,baseline,list(targets));st.save_json(folder/'resource-samples.json',{'baselineCpuUsec':baseline,'samples':samples})
   record=dict(trial,status='verified',containerId=cid,containerWallSeconds=wall,resources=summary,driverExecution=value['execution'],executorEvidence=value['executorEvidence'],metrics=value['metrics'],anomalySha256=value['anomalySha256'])
   st.save_json(folder/'measurement.json',record);self.wait_empty();self.idle();return record
  finally:
   self.owned(cid)
   if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':st.run(['docker','stop','--time','5',cid],timeout=20)
   if waiter and waiter.poll() is None:waiter.kill();waiter.communicate(timeout=5)
   if not (folder/'driver.log').exists():self.logs(cid,folder/'driver.log')

def aggregate(records):
 groups=[]
 for rows,profile in sorted({(v['rows'],v['profile']) for v in records if not v['warmup']}):
  for mode in ('local','cluster'):
   values=[v for v in records if not v['warmup'] and (v['rows'],v['profile'],v['mode'])==(rows,profile,mode)]
   if not values:raise ValueError('Incomplete paired modes')
   groups.append({'rows':rows,'profile':profile,'mode':mode,'n':len(values),'medianWallSeconds':statistics.median(v['containerWallSeconds'] for v in values),'minWallSeconds':min(v['containerWallSeconds'] for v in values),'maxWallSeconds':max(v['containerWallSeconds'] for v in values),'medianSampledCpuSecondsLowerBound':statistics.median(v['resources']['sampledCpuSecondsLowerBound'] for v in values),'medianSampledMemoryPeakMiB':statistics.median(v['resources']['sampledAggregateMemoryPeakBytes']/1024**2 for v in values)})
 return groups

def run(args):
 trials=plan(args.rows,args.repeats,args.seed);assert 60<=args.max_seconds<=1800
 root=args.output.absolute();root.resolve().relative_to(st.ROOT/'.runtime');assert not root.exists() and root.parent.is_dir();host=json.loads(args.host_capacity.read_text(encoding='utf-8-sig'));assert host.get('known') and host['freeBytes']>=8*1024**3 and shutil.disk_usage(root.parent).free>=8*1024**3
 # Only primary idle containers may coexist: do not benchmark while another lab is running.
 allowed={st.Project().service(role) for role in ('mysql','backend','frontend')};running=set(st.run(['docker','ps','-q','--no-trunc']).decode().split());assert running.issubset(allowed),'Other containers running; stop owned lab before comparison'
 with (st.ROOT/'.runtime/cluster-compare.lock').open('a') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);before=b.source_snapshot();point=st.recovery_point(st.Project());root.mkdir(mode=0o700)
  sources={str(p.relative_to(st.ROOT)):st.digest(p) for p in [Path(__file__),st.ROOT/'infrastructure/standalone_trial.py',st.ROOT/'infrastructure/benchmark.py',st.ROOT/'spark-engine/target/spark-engine-0.1.0.jar']}
  st.save_json(root/'primary-before.json',before);st.save_json(root/'primary-point.json',point);st.save_json(root/'plan.json',trials)
  (root/'inputs').mkdir(mode=0o755);(root/'output').mkdir(mode=0o755);os.chown(root/'output',185,0);shutil.copyfile(st.ROOT/'spark-engine/target/spark-engine-0.1.0.jar',root/'app.jar');os.chmod(root/'app.jar',0o444)
  datasets={}
  for n in args.rows:datasets[n]=b.generate(root/'inputs'/(str(n)+'.csv'),n,args.seed)
  image=st.run(['docker','image','inspect','--format','{{.Id}}',s.IMAGE]).decode().strip();session=uuid.uuid4().hex;cluster=Comparison(root,session);cluster.seed=args.seed
  st.save_json(root/'environment.json',{'image':s.IMAGE,'imageId':image,'sourceDigests':sources,'datasets':datasets,'hostCapacity':host,'cpuBudget':{'local':2,'clusterDriver':.5,'clusterWorkers':.75,'masterAdditional':.5},'memoryBudgetMiB':{'local':5120,'clusterDriver':2048,'clusterWorkers':1536,'masterAdditional':512},'heapMiB':{'local':2560,'clusterDriver':1024,'clusterExecutor':768},'inputPartitions':8,'shufflePartitions':2,'aqe':False,'sampleIntervalSeconds':.25,'deadlineSeconds':args.max_seconds,'session':session})
  records=[];deadline=time.monotonic()+args.max_seconds
  try:
   cluster.start_cluster()
   for trial in trials:
    if time.monotonic()>deadline:raise st.StorageError('Group deadline exceeded')
    b.source_idle();record=cluster.measure(trial,deadline)
    counterpart=next((v for v in records if (v['rows'],v['profile'],v['repeat'],v['warmup'])==(trial['rows'],trial['profile'],trial['repeat'],trial['warmup']) and v['mode']!=trial['mode']),None)
    if counterpart:assert record['metrics']==counterpart['metrics'] and record['anomalySha256']==counterpart['anomalySha256'],'Paired result mismatch'
    records.append(record);print(json.dumps({k:record[k] for k in ('index','rows','profile','mode','warmup','containerWallSeconds')}) ,flush=True)
  finally:cluster.close()
  assert before==b.source_snapshot() and point['sha256']==st.recovery_point(st.Project())['sha256'];assert all(st.digest(st.ROOT/name)==digest for name,digest in sources.items());assert all(st.digest(root/'inputs'/(str(n)+'.csv'))=={'sha256':value['sha256'],'size':value['size']} for n,value in datasets.items())
  summary={'ok':True,'groups':aggregate(records),'measuredTrials':sum(not v['warmup'] for v in records),'warmupTrials':sum(v['warmup'] for v in records),'primaryUnchanged':True,'allOwnedContainersStopped':True,'networkRemoved':not cluster.network_created,'scope':'single-host equal workload-side aggregate quotas; extra cluster Master; fresh driver JVM, reused cluster daemons; sampled CPU lower bound, sampled summed memory peak; no HA/cross-host claim'}
  st.save_json(root/'measurements.json',records);st.save_json(root/'summary.json',summary)
  with (root/'summary.csv').open('x',newline='') as stream:
   writer=csv.DictWriter(stream,fieldnames=list(summary['groups'][0]));writer.writeheader();writer.writerows(summary['groups'])
  st.save_json(root/'COMPLETE',{'summary':st.digest(root/'summary.json'),'measurements':st.digest(root/'measurements.json')});print(json.dumps(summary),flush=True)

def main():
 def interrupted(signum,frame):raise InterruptedError('Interrupted; own cluster cleanup follows')
 signal.signal(signal.SIGTERM,interrupted);p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--host-capacity',type=Path,required=True);p.add_argument('--rows',type=int,nargs='+',default=[10000,200000]);p.add_argument('--repeats',type=int,default=2);p.add_argument('--seed',type=int,default=b.SEED);p.add_argument('--max-seconds',type=int,default=1800);run(p.parse_args())
if __name__=='__main__':main()
