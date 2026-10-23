#!/usr/bin/env python3
"""Paired three-format worker benchmark; preparation cost is explicit, never a production cache rollout."""
import argparse,csv,decimal,fcntl,json,math,os,random,shutil,signal,statistics,time,uuid
from pathlib import Path
import benchmark as b,storage as st,runtime as rt,cluster_compare as c
FORMATS=('CSV_SINGLE','CSV','PARQUET')
def plan(rows,repeats,seed):
 c.plan(rows,repeats,seed);rng=random.Random(seed)
 pairs=[dict(rows=n,profile=p,repeat=i,warmup=False,mode=m) for n in rows for p in ('extended','governance') for i in range(1,repeats+1) for m in ('local','cluster')];rng.shuffle(pairs)
 trials=[dict(rows=min(rows),profile='extended',repeat=0,warmup=True,mode=m,format='CSV_SINGLE') for m in ('local','cluster')]
 for pair in pairs:
  formats=list(FORMATS);rng.shuffle(formats);trials.extend(dict(pair,format=kind) for kind in formats)
 if len(trials)>50:raise ValueError('At most 50 trials including warmups')
 return [dict(value,index=i) for i,value in enumerate(trials,1)]
def break_even(preparation,baseline,prepared):
 values=[decimal.Decimal(str(value)) for value in (preparation,baseline,prepared)]
 if any(not v.is_finite() or v<0 for v in values):raise ValueError('Nonnegative finite timings required')
 cost,base,worker=values;difference=base-worker
 return None if difference<=0 else int(cost//difference)+1

def summarize(records,preparations):
 groups=[]
 for rows,profile,mode in sorted({(v['rows'],v['profile'],v['mode']) for v in records if not v['warmup']}):
  selected=[v for v in records if not v['warmup'] and (v['rows'],v['profile'],v['mode'])==(rows,profile,mode)]
  counts=[sum(v['format']==kind for v in selected) for kind in FORMATS]
  if not counts[0] or len(set(counts))!=1:raise ValueError('Incomplete or unbalanced format group')
  baseline=statistics.median(v['containerWallSeconds'] for v in selected if v['format']=='CSV_SINGLE')
  for kind in FORMATS:
   values=[v for v in selected if v['format']==kind];wall=statistics.median(v['containerWallSeconds'] for v in values);prep=0 if kind=='CSV_SINGLE' else preparations[str(rows)+'/'+kind]['wallSeconds']
   groups.append({'rows':rows,'profile':profile,'mode':mode,'format':kind,'n':len(values),'medianWallSeconds':wall,'minWallSeconds':min(v['containerWallSeconds'] for v in values),'maxWallSeconds':max(v['containerWallSeconds'] for v in values),'medianReadSeconds':statistics.median(v['driverExecution']['inputReadAndCountMs']/1000 for v in values),'sourcePartitions':sorted({v['driverExecution']['sourcePartitions'] for v in values}),'medianSampledCpuSecondsLowerBound':statistics.median(v['resources']['sampledCpuSecondsLowerBound'] for v in values),'medianSampledMemoryPeakMiB':statistics.median(v['resources']['sampledAggregateMemoryPeakBytes']/1024**2 for v in values),'preparationWallSeconds':prep,'preparePlusOneRunSeconds':prep+wall,'strictBreakEvenRuns':None if kind=='CSV_SINGLE' else break_even(prep,baseline,wall),'modeledSecondsForFiveRuns':prep+5*wall})
 return groups

class Formats(c.Comparison):
 def idle(self):
  b.source_idle()
  if rt.active(st.Project()):raise st.StorageError('Primary task or import active; own experiment stops')
 def input_request(self,trial):
  if trial['format']=='CSV_SINGLE':return super().input_request(trial)
  key=str(trial['rows'])+'/'+trial['format'];folder=self.root/'inputs'/(str(trial['rows'])+'-'+trial['format'].lower());manifest=folder/'manifest.json';pin=self.preparations[key]['manifestSha256'];assert st.digest(manifest)['sha256']==pin
  return {'inputManifest':'/cluster/input/'+folder.name+'/manifest.json','inputManifestSha256':pin}
 def check_input(self,trial,execution):
  assert execution['inputFormat']==trial['format'] and execution['inputFiles']==(1 if trial['format']=='CSV_SINGLE' else 4)
  assert type(execution['sourcePartitions']) is int and execution['sourcePartitions']>0
  assert execution['repartitionApplied']==(execution['sourcePartitions']!=8)
  if trial['format']=='CSV_SINGLE':assert execution['sourcePartitions']==1 and 'inputManifestSha256' not in execution
  else:assert execution['inputManifestSha256']==self.preparations[str(trial['rows'])+'/'+trial['format']]['manifestSha256']
 def prepare(self,rows,kind):
  self.idle();name='local-prepare-'+str(rows)+'-'+kind.lower();folder=self.folder(name);payload={'input':'/cluster/input/'+str(rows)+'.csv','output':'/cluster/output/'+name+'/prepared','format':kind,'partitions':4};st.save_json(folder/'request.json',payload);os.chown(folder/'request.json',185,0)
  command=['/opt/spark/bin/spark-submit','--master','local[2]','--driver-memory','2560m','--conf','spark.sql.adaptive.enabled=false','--conf','spark.locality.wait=0s','--class','cn.sparkdq.engine.PreparedInput','/cluster/app.jar','/cluster/output/'+name+'/request.json']
  cid=self.create(name,command,network=False,extra=['--add-host',name+':127.0.0.1','-e','SPARK_MASTER=local[2]','-e','SPARK_LOCAL_IP=127.0.0.1']);started=time.monotonic()
  try:
   with (folder/'attach.log').open('xb') as stream:st.run(['docker','start','-a',cid],output=stream,timeout=310)
   wall=time.monotonic()-started;state=json.loads(st.run(['docker','inspect','--format','{{json .State}}',cid]));assert not state['Running'] and not state['OOMKilled'] and state['ExitCode']==0
   proof=json.loads((folder/'prepared/preparation.json').read_text());assert proof['verified'] and proof['rows']==rows and proof['files']==4
   destination=self.root/'inputs'/(str(rows)+'-'+kind.lower());shutil.copytree(folder/'prepared',destination)
   for path in destination.rglob('*'):
    if path.is_file():os.chmod(path,0o444)
   manifest=json.loads((destination/'manifest.json').read_text());assert manifest['sourceSha256']==st.digest(self.root/'inputs'/(str(rows)+'.csv'))['sha256'];assert proof['manifestSha256']==st.digest(destination/'manifest.json')['sha256']
   result=dict(proof,wallSeconds=wall,containerId=cid,preparedBytes=sum(entry['size'] for entry in manifest['files']));st.save_json(folder/'verified.json',result);self.idle();return result
  finally:
   self.owned(cid)
   if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':st.run(['docker','stop','--time','5',cid],timeout=20)
   self.logs(cid,folder/'driver.log')

def fingerprint(folder):return {str(p.relative_to(folder)):st.digest(p) for p in sorted(folder.rglob('*')) if p.is_file()}
def run(args):
 trials=plan(args.rows,args.repeats,args.seed);assert 120<=args.max_seconds<=2400
 root=args.output.absolute();root.resolve().relative_to(st.ROOT/'.runtime');assert not root.exists() and root.parent.is_dir();capacity=json.loads(args.host_capacity.read_text(encoding='utf-8-sig'));assert capacity.get('known') and capacity['freeBytes']>=8*1024**3 and shutil.disk_usage(root.parent).free>=8*1024**3
 allowed={st.Project().service(role) for role in ('mysql','backend','frontend')};assert set(st.run(['docker','ps','-q','--no-trunc']).decode().split()).issubset(allowed),'Other containers running'
 with (st.ROOT/'.runtime/cluster-compare.lock').open('a') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);assert rt.active(st.Project())==0;before=b.source_snapshot();point=st.recovery_point(st.Project());root.mkdir(mode=0o700)
  st.save_json(root/'primary-before.json',before);st.save_json(root/'primary-point.json',point);st.save_json(root/'plan.json',trials)
  (root/'inputs').mkdir(mode=0o755);(root/'output').mkdir(mode=0o755);os.chown(root/'output',185,0);shutil.copyfile(st.ROOT/'spark-engine/target/spark-engine-0.1.0.jar',root/'app.jar');os.chmod(root/'app.jar',0o444)
  datasets={n:b.generate(root/'inputs'/(str(n)+'.csv'),n,args.seed) for n in args.rows};sources={str(p.relative_to(st.ROOT)):st.digest(p) for p in [Path(__file__),st.ROOT/'infrastructure/cluster_compare.py',st.ROOT/'infrastructure/benchmark.py',st.ROOT/'infrastructure/standalone_trial.py',st.ROOT/'spark-engine/src/main/java/cn/sparkdq/engine/Engine.java',st.ROOT/'spark-engine/target/spark-engine-0.1.0.jar']}
  image=st.run(['docker','image','inspect','--format','{{.Id}}',c.s.IMAGE]).decode().strip();session=uuid.uuid4().hex;cluster=Formats(root,session);cluster.seed=args.seed;cluster.preparations={};records=[];deadline=time.monotonic()+args.max_seconds
  st.save_json(root/'environment.json',{'image':c.s.IMAGE,'imageId':image,'session':session,'sourceDigests':sources,'datasets':datasets,'hostCapacity':capacity,'seed':args.seed,'inputPartitions':8,'shufflePartitions':2,'preparedFiles':4,'aqe':False,'localityWait':'0s','cpuBudget':{'local':2,'driver':.5,'eachWorker':.75,'masterExtra':.5},'memoryMiB':{'local':5120,'driver':2048,'eachWorker':1536,'masterExtra':512},'requestedDriverHeap':{'local':'2560m','cluster':'1g'},'requestedExecutorHeap':'768m','preparationMode':'local[2] / 2CPU / 5GiB / requested heap2560m','reuseModelOnly':True})
  try:
   for n in args.rows:
    for kind in ('CSV','PARQUET'):
     if time.monotonic()>deadline:raise st.StorageError('Preparation group deadline exceeded')
     cluster.preparations[str(n)+'/'+kind]=cluster.prepare(n,kind);print('PASS prepare '+str(n)+'/'+kind,flush=True)
   st.save_json(root/'preparations.json',cluster.preparations);inputs=fingerprint(root/'inputs');cluster.start_cluster()
   for trial in trials:
    if time.monotonic()>deadline:raise st.StorageError('Group deadline exceeded')
    cluster.idle();record=cluster.measure(trial,deadline)
    for peer in records:
     if (peer['rows'],peer['profile'])==(trial['rows'],trial['profile']):assert peer['metrics']==record['metrics'] and peer['anomalySha256']==record['anomalySha256'],'Format/mode/repeat result mismatch'
    records.append(record);print(json.dumps({k:record[k] for k in ('index','rows','profile','mode','format','warmup','containerWallSeconds')}),flush=True)
  finally:cluster.close()
  assert before==b.source_snapshot() and point['sha256']==st.recovery_point(st.Project())['sha256'] and rt.active(st.Project())==0
  assert inputs==fingerprint(root/'inputs') and all(st.digest(st.ROOT/name)==value for name,value in sources.items())
  summary={'ok':True,'groups':summarize(records,cluster.preparations),'measuredTrials':sum(not v['warmup'] for v in records),'warmupTrials':sum(v['warmup'] for v in records),'preparations':cluster.preparations,'primaryUnchanged':True,'allOwnedContainersStopped':True,'networkRemoved':not cluster.network_created,'scope':'same-host 200k default, three formats, 2CPU/5GiB aggregate workload quotas + Master; preparation separate; reuse/break-even are models, no production shared cache implemented; n=2 descriptive'}
  st.save_json(root/'measurements.json',records);st.save_json(root/'summary.json',summary)
  with (root/'summary.csv').open('x',newline='') as stream:
   writer=csv.DictWriter(stream,fieldnames=list(summary['groups'][0]));writer.writeheader();writer.writerows(summary['groups'])
  st.save_json(root/'COMPLETE',{'summary':st.digest(root/'summary.json'),'measurements':st.digest(root/'measurements.json')});print(json.dumps(summary),flush=True)
def main():
 def interrupted(signum,frame):raise InterruptedError('Interrupted; own cluster cleanup follows')
 signal.signal(signal.SIGTERM,interrupted);p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--host-capacity',type=Path,required=True);p.add_argument('--rows',type=int,nargs='+',default=[200000]);p.add_argument('--repeats',type=int,default=2);p.add_argument('--seed',type=int,default=b.SEED);p.add_argument('--max-seconds',type=int,default=2400);run(p.parse_args())
if __name__=='__main__':main()
