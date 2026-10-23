#!/usr/bin/env python3
"""Bounded offline IQR/FD scale experiment; no HTTP/product capacity claim."""
import contextlib,hashlib,json,os,shutil,signal,statistics,subprocess,sys,time,traceback,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'infrastructure')]
import storage as st,cluster_compare as comparison,standalone_trial as standalone,scale_oracle as oracle

def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2))
def digest(path):
 h=hashlib.sha256()
 with path.open('rb') as stream:
  for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
 return h.hexdigest()
def disk_io(path):
 try:
  values={}
  for line in (path/'io.stat').read_text().splitlines():
   for entry in line.split()[1:]:
    key,value=entry.split('=');values[key]=values.get(key,0)+int(value)
  return values
 except FileNotFoundError:return None
def events(folder):
 metrics={};sql={};stages=[]
 for path in folder.glob('*'):
  if not path.is_file() or path.name.startswith('.'):continue
  with path.open() as stream:
   for line in stream:
    e=json.loads(line);kind=e.get('Event','')
    if kind=='SparkListenerTaskEnd':
     m=e.get('Task Metrics',{});values={key:m.get(key,0) for key in ('Executor Run Time','Executor CPU Time','JVM GC Time','Memory Bytes Spilled','Disk Bytes Spilled')}
     values.update(shuffleReadBytes=sum(m.get('Shuffle Read Metrics',{}).get(key,0) for key in ('Remote Bytes Read','Local Bytes Read')),shuffleWriteBytes=m.get('Shuffle Write Metrics',{}).get('Shuffle Bytes Written',0),outputBytes=m.get('Output Metrics',{}).get('Bytes Written',0),inputBytes=m.get('Input Metrics',{}).get('Bytes Read',0))
     for k,v in values.items():metrics[k]=metrics.get(k,0)+v
    elif kind.endswith('SparkListenerSQLExecutionStart'):
     plan=e.get('physicalPlanDescription','');sql[e['executionId']]=dict(start=e['time'],description=e.get('description',''),plan=plan,containsSort='Sort' in plan,containsExchange='Exchange' in plan,containsCache='InMemory' in plan)
    elif kind.endswith('SparkListenerSQLExecutionEnd') and e['executionId'] in sql:sql[e['executionId']]['wallMilliseconds']=e['time']-sql[e['executionId']]['start']
    elif kind=='SparkListenerStageCompleted':
     info=e['Stage Info'];stages.append({k:info.get(k) for k in ('Stage ID','Stage Name','Submission Time','Completion Time','Number of Tasks','Failure Reason')})
 return dict(taskMetrics=metrics,sqlExecutions=list(sql.values()),stages=stages,note='Task totals include actual attempts. SQL plan categories overlap and must not be added as independent costs. Event logs retained in full.')
class Trial(comparison.Comparison):
 def close(self):
  for role,cid in reversed(self.containers):
   self.owned(cid)
   if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':st.run(['docker','stop','--time','5',cid],timeout=20)
   path=self.root/(role+'-final.log')
   if not path.exists():self.logs(cid,path)
  # Historical networks and all output are intentionally retained.
 def measure(self,n,index,mode,expected,deadline,warmup):
  name=mode+'-'+str(n)+'-'+str(index);folder=self.folder(name);remote=mode=='cluster';event=folder/'eventlog';event.mkdir();os.chown(event,185,0)
  payload=dict(taskId=name,input='/cluster/input/'+str(n)+'.csv',output='/cluster/output/'+name+'/result',semanticsVersion='csv-v2',rules=oracle.RULES);save(folder/'request.json',payload)
  master='spark://master:7077' if remote else 'local[2]';command=['/opt/spark/bin/spark-submit','--master',master,'--deploy-mode','client','--driver-memory','1g' if remote else '2560m']
  conf={'spark.locality.wait':'0s','spark.ui.enabled':'false','spark.sql.adaptive.enabled':'false','spark.sql.shuffle.partitions':'2','spark.executor.memory':'768m','spark.executor.cores':'1','spark.cores.max':'2','spark.dynamicAllocation.enabled':'false','spark.scheduler.minRegisteredResourcesRatio':'1','spark.scheduler.maxRegisteredResourcesWaitingTime':'30s','spark.extraListeners':'cn.sparkdq.engine.ClusterEvidence','spark.eventLog.enabled':'true','spark.eventLog.compress':'false','spark.eventLog.dir':'file:///cluster/output/'+name+'/eventlog'}
  for k,v in conf.items():command+=['--conf',k+'='+v]
  command+=['--class','cn.sparkdq.engine.Engine','/cluster/app.jar','/cluster/output/'+name+'/request.json'];env=['-e','SPARK_MASTER='+master,'-e','SPARK_INPUT_PARTITIONS=8','-e','SPARK_SHUFFLE_PARTITIONS=2','-e','SPARK_DQ_EVIDENCE=/cluster/output/'+name+'/executor-evidence.json']
  env+=['-e','SPARK_DQ_DRIVER_HOST=driver','--network-alias','driver'] if remote else ['-e','SPARK_LOCAL_IP=127.0.0.1','--add-host',name+':127.0.0.1']
  cid=self.create(name,command,network=remote,extra=env);targets={role:comparison.cgroup(value) for role,value in self.containers if remote and role in ('master','worker1','worker2')};baseline={role:comparison.read_cgroup(path)['cpuUsec'] for role,path in targets.items()};io_baseline={role:disk_io(path) for role,path in targets.items()};samples=[];started=time.monotonic();waiter=None;record=dict(rows=n,index=index,mode=mode,warmup=warmup,status='failed',containerId=cid)
  try:
   st.run(['docker','start',cid]);targets['driver']=comparison.cgroup(cid);waiter=subprocess.Popen(['docker','wait',cid],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
   while True:
    samples.append({'seconds':time.monotonic()-started,'roles':{role:v for role,path in targets.items() if (v:=comparison.read_cgroup(path)) is not None},'reportedDeviceIo':{role:v for role,path in targets.items() if (v:=disk_io(path)) is not None}})
    if waiter.poll() is not None:break
    if time.monotonic()-started>300 or time.monotonic()>deadline:raise TimeoutError('Single run 300s or batch deadline reached')
    time.sleep(.25)
   stdout,stderr=waiter.communicate(timeout=5);record['containerWallSeconds']=time.monotonic()-started
   state=json.loads(st.run(['docker','inspect','--format','{{json .State}}',cid]));record['containerState']=state;assert waiter.returncode==0 and stdout.strip()==b'0' and not state['OOMKilled'],(stdout,stderr)
   result=json.loads((folder/'result/result.json').read_text());assert result['totalRows']==n;actual={r['ruleId']:r['metrics'] for r in result['results']};assert actual==expected['metrics'],(actual,expected['metrics'])
   verify_started=time.monotonic();hashes={}
   for rule in ('iqr','fd'):
    raw=folder/(rule+'.actual.unsorted')
    with raw.open('w') as out:
     for part in (folder/'result/anomalies'/rule).glob('part-*.json'):
      with part.open() as stream:
       for line in stream:
        value=json.loads(line);assert set(value)==set(oracle.COLUMNS);out.write(oracle.encode(value))
    hashes[rule]=oracle.sorted_digest(raw,folder/(rule+'.actual.sorted'));assert hashes[rule]==expected['anomalies'][rule],rule
   proof=standalone.evidence(folder/'executor-evidence.json',self.hosts if remote else None)
   if remote:assert {x['host'] for x in proof['executors']}=={'worker1','worker2'}
   record.update(status='verified',metrics=actual,anomalies=hashes,executorEvidence=proof,verificationAndSortSeconds=time.monotonic()-verify_started,resources=comparison.resources(samples,baseline,list(targets)),execution=result['execution'],eventMetrics=events(event));self.wait_empty()
  except Exception:
   record['error']=traceback.format_exc();raise
  finally:
   self.owned(cid)
   if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':st.run(['docker','stop','--time','5',cid],timeout=20)
   if waiter and waiter.poll() is None:waiter.kill();waiter.communicate(timeout=5)
   if not (folder/'driver.log').exists():self.logs(cid,folder/'driver.log')
   save(folder/'resource-samples.json',dict(baselineCpuUsec=baseline,baselineReportedDeviceIo=io_baseline,samples=samples,diskNote='Cgroup-reported sums across devices; sampled endpoint may miss final writes; not exact physical-disk traffic.'));save(folder/'measurement.json',record)
  return record
def main():
 def interrupted(signum,frame):raise TimeoutError('Experiment interrupted or hard batch deadline reached')
 signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGALRM,interrupted);signal.alarm(2800)
 root=ROOT/'.runtime'/('t4-scale-'+time.strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:6]);root.mkdir(mode=0o700);(root/'inputs').mkdir();(root/'output').mkdir();os.chown(root/'output',185,0);shutil.copyfile(ROOT/'spark-engine/target/spark-engine-0.1.0.jar',root/'app.jar');(root/'app.jar').chmod(0o444)
 assert not st.run(['docker','ps','-q']).strip(),'Other experiments must stop before measurement'
 summary=dict(ok=False,scope='Single physical host; offline entry only; no product limit increase or cross-host speedup claim',groups=[],failures=[]);cluster=Trial(root,uuid.uuid4().hex);started=time.monotonic();deadline=started+2700;records=[]
 try:
  available=int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')))*1024;assert available>=6*1024**3
  save(root/'environment.json',dict(jarSha256=digest(root/'app.jar'),sparkImage=standalone.IMAGE,sparkVersion='3.5.8',scala='2.12',java=17,inputPartitions=8,shufflePartitions=2,heapMiB={'localDriver':2560,'clusterDriver':1024,'eachExecutor':768},cpuQuotas={'local':2,'clusterDriver':.5,'eachWorker':.75,'masterAdditional':.5},memoryQuotasMiB={'local':5120,'clusterDriver':2048,'eachWorker':1536,'masterAdditional':512},singleRunSeconds=300,batchSeconds=2700,availableMemoryBytes=available,sampleIntervalSeconds=.25,localSamplingScope='Driver only; idle cluster daemons coexist and are excluded. Cluster sampling includes Driver, Master and both Workers. Extra deployment overhead is explicit.',retention='Full input, anomalies, externally sorted multisets, SQLite oracle, event logs, failures, containers and networks retained'))
  cluster.start_cluster()
  trial_expected=oracle.prepare(root/'oracle-trial-20000',20000);shutil.copyfile(root/'oracle-trial-20000/input.csv',root/'inputs/20000.csv')
  for mode in ('local','cluster'):
   record=cluster.measure(20000,-1,mode,trial_expected,deadline,True);record['trialOnly']=True;records.append(record);save(root/'measurements.json',records);print(json.dumps(dict(trialOnly=True,mode=mode,rows=20000,status=record['status'])),flush=True)
  for n in (200000,1000000,5000000):
   admission=dict(rows=n,hostFreeBytes=shutil.disk_usage('/mnt/c').free,wslFreeBytes=shutil.disk_usage(root).free,requiredFreeBytes=max(8*1024**3,n*120*16),remainingBatchSeconds=deadline-time.monotonic(),wslAvailableMemoryBytes=int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')))*1024);save(root/('admission-'+str(n)+'.json'),admission)
   if min(admission['hostFreeBytes'],admission['wslFreeBytes'])<admission['requiredFreeBytes'] or admission['remainingBatchSeconds']<600 or admission['wslAvailableMemoryBytes']<5*1024**3:summary['stoppedAtAdmission']=admission;break
   expected=oracle.prepare(root/('oracle-'+str(n)),n);shutil.copyfile(root/('oracle-'+str(n))/'input.csv',root/'inputs'/(str(n)+'.csv'));save(root/('input-'+str(n)+'.json'),dict(expected,sha256=digest(root/'inputs'/(str(n)+'.csv'))));current=[]
   for index in range(4):
    for mode in (('local','cluster') if index%2==0 else ('cluster','local')):
     record=cluster.measure(n,index,mode,expected,deadline,index==0);records.append(record);current.append(record);save(root/'measurements.json',records);print(json.dumps({k:record[k] for k in ('rows','index','mode','warmup','containerWallSeconds','status')}),flush=True)
   for mode in ('local','cluster'):
    values=[r for r in current if r['mode']==mode and not r['warmup']];times=[r['containerWallSeconds'] for r in values];summary['groups'].append(dict(rows=n,inputBytes=expected['inputBytes'],mode=mode,formalMeasurements=len(values),warmup=1,medianWallSeconds=statistics.median(times),minWallSeconds=min(times),maxWallSeconds=max(times)))
   save(root/'progress.json',summary)
  summary.update(ok=bool(summary['groups']),maximumVerifiedRows=max((g['rows'] for g in summary['groups']),default=0),exceeded20MiB=any(g['inputBytes']>20*1024**2 for g in summary['groups']))
 except Exception:
  summary['failures'].append(traceback.format_exc());(root/'failure.txt').write_text(summary['failures'][-1]);summary['maximumVerifiedRows']=max((g['rows'] for g in summary['groups']),default=0)
 finally:
  with contextlib.suppress(Exception):cluster.close()
  summary.update(totalSeconds=time.monotonic()-started,allOwnedContainersStopped=all(st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'false' for _,cid in cluster.containers),networksAndVolumesRetained=True);save(root/'summary.json',summary);print(json.dumps(dict(root=str(root),**summary)),flush=True)
if __name__=='__main__':main()
