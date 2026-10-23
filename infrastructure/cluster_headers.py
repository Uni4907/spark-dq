#!/usr/bin/env python3
"""Bounded paired prepared-CSV header optimization acceptance; no HTTP task writes."""
import argparse,fcntl,json,os,random,shutil,signal,statistics,subprocess,time,uuid
from pathlib import Path
import benchmark as b,storage as st,runtime as rt,cluster_formats as f
VARIANTS=('baseline','candidate')
def semantic_metrics(results):return {row['ruleId']:row['metrics'] for row in results}
def plan(repeats=2,seed=b.SEED):
 if type(repeats) is not int or repeats not in (1,2,3):raise ValueError('1..3 repeats required')
 rng=random.Random(seed);trials=[]
 for mode in ('local','cluster'):
  for variant in VARIANTS:trials.append(dict(mode=mode,variant=variant,profile='extended',repeat=0,warmup=True))
 pairs=[(mode,profile,i) for mode in ('local','cluster') for profile in ('extended','governance') for i in range(1,repeats+1)];rng.shuffle(pairs)
 for mode,profile,i in pairs:
  variants=list(VARIANTS);rng.shuffle(variants)
  for variant in variants:trials.append(dict(mode=mode,variant=variant,profile=profile,repeat=i,warmup=False))
 return [dict(t,index=i,rows=200000,format='CSV') for i,t in enumerate(trials,1)]
def summarize(records):
 groups=[]
 for mode in ('local','cluster'):
  for profile in ('extended','governance'):
   selected=[v for v in records if not v['warmup'] and (v['mode'],v['profile'])==(mode,profile)]
   values={variant:[v for v in selected if v['variant']==variant] for variant in VARIANTS}
   if not values['baseline'] or len(values['baseline'])!=len(values['candidate']):raise ValueError('Incomplete A/B group')
   for variant,rows in values.items():
    wall=[v['containerWallSeconds'] for v in rows];read=[v['driverExecution']['inputReadAndCountMs']/1000 for v in rows]
    groups.append(dict(mode=mode,profile=profile,variant=variant,n=len(rows),medianWallSeconds=statistics.median(wall),minWallSeconds=min(wall),maxWallSeconds=max(wall),medianReadSeconds=statistics.median(read),minReadSeconds=min(read),maxReadSeconds=max(read)))
 return groups
class Headers(f.Formats):
 def select_jar(self,variant):
  assert variant in VARIANTS
  self.wait_empty();self.idle()
  # Replacing an inode while a driver/executor uses it would make the A/B comparison invalid.
  for role,cid in self.containers:
   if role.startswith(('local-','cluster-')):assert st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'false'
  shutil.copyfile(self.root/(variant+'.jar'),self.root/'app.jar');os.chmod(self.root/'app.jar',0o444)
  assert st.digest(self.root/'app.jar')==st.digest(self.root/(variant+'.jar'))
 def compatibility(self):
  records=[]
  for variant in VARIANTS:
   self.select_jar(variant)
   for case in ('plain','quoted','crlf','bom','mixed','multiline-values','reordered','prefix','second-part-wrong','bad-checksum','bad-row-count'):
    name='local-compat-'+variant+'-'+case;folder=self.folder(name);src=self.root/'inputs'/('compat-'+case)
    payload=dict(taskId=name,output='/cluster/output/'+name+'/result',semanticsVersion='csv-v2',rules=[{'id':'blank-name','type':'NOT_NULL','column':'name'}],inputManifest='/cluster/input/'+src.name+'/manifest.json',inputManifestSha256=st.digest(src/'manifest.json')['sha256'])
    # Use the normal Engine payload schema, not a test-only reader.
    payload['rules']=[dict(b.rules('extended')[0]),dict(b.rules('extended')[2])]
    st.save_json(folder/'request.json',payload);os.chown(folder/'request.json',185,0)
    cmd=['/opt/spark/bin/spark-submit','--master','local[2]','--driver-memory','1g','--class','cn.sparkdq.engine.Engine','/cluster/app.jar','/cluster/output/'+name+'/request.json']
    cid=self.create(name,cmd,network=False,extra=['-e','SPARK_MASTER=local[2]','-e','SPARK_LOCAL_IP=127.0.0.1','--add-host',name+':127.0.0.1'])
    with (folder/'attach.log').open('xb') as stream:
     attached=subprocess.run(['docker','start','-a',cid],stdout=stream,stderr=subprocess.STDOUT,timeout=150,check=False)
    assert attached.returncode in (0,1),'Unexpected Docker attach failure'
    state=json.loads(st.run(['docker','inspect','--format','{{json .State}}',cid]));self.logs(cid,folder/'driver.log');assert not state['Running'] and not state['OOMKilled']
    success=case in ('plain','quoted','crlf','bom','mixed','multiline-values');result=folder/'result/result.json'
    if success:
     assert state['ExitCode']==0 and result.exists(),(variant,case,str(folder))
     value=json.loads(result.read_text());assert value['totalRows']==2 and value['results'][0]['metrics']['blankCount']==0
     metrics=value['results'];assert metrics[1]['metrics']['duplicateRows']==2
     rows=[json.loads(line) for path in (folder/'result/anomalies/duplicate-id').glob('part-*.json') for line in path.read_text().splitlines()]
     expected_names=['A,\"中\"\nnext','B'] if case=='multiline-values' else ['A','B']
     assert sorted(row['name'] for row in rows)==sorted(expected_names) and all(row['id']=='1' and row['__dq_group_size']==2 for row in rows)
     assert all(not path.read_text().strip() for path in (folder/'result/anomalies/missing-name').glob('part-*.json'))
    else:
     assert state['ExitCode']!=0 and not result.exists(),(variant,case,str(folder));metrics=None
     log=(folder/'driver.log').read_text();expected={'bad-checksum':'Prepared file checksum mismatch','bad-row-count':'row count','reordered':'Prepared CSV header mismatch','prefix':'Prepared CSV header mismatch','second-part-wrong':'Prepared CSV header mismatch'}[case]
     assert expected.lower() in log.lower(),(case,log[-2000:])
    record=dict(variant=variant,case=case,success=success,exitCode=state['ExitCode'],metrics=semantic_metrics(metrics) if metrics is not None else None);records.append(record);st.save_json(self.root/('compatibility-'+variant+'-'+case+'.json'),record);print('PASS compatibility '+variant+'/'+case,flush=True)
  for case in {v['case'] for v in records}:
   peers=[v for v in records if v['case']==case];assert len(peers)==2 and peers[0]['success']==peers[1]['success'] and peers[0]['metrics']==peers[1]['metrics']
  st.save_json(self.root/'compatibility.json',records)
  return records

def fixtures(root):
 for case in ('plain','quoted','crlf','bom','mixed','multiline-values','reordered','prefix','second-part-wrong','bad-checksum','bad-row-count'):
  folder=root/('compat-'+case);(folder/'data').mkdir(parents=True)
  header={'quoted':'"id","name"','mixed':'id,"name"','bom':'\ufeffid,name','reordered':'name,id','prefix':'id,nameExtra'}.get(case,'id,name');ending='\r\n' if case=='crlf' else '\n';body='1,A'+ending+'1,B'+ending
  if case=='multiline-values':body='1,"A,\"\"中\"\"\nnext"\n1,B\n'
  entries=[]
  for i in range(2 if case=='second-part-wrong' else 1):
   path=folder/'data'/('part-'+str(i)+'.csv');path.write_bytes(((header if i==0 else 'id,other')+ending+body).encode('utf-8'));entries.append(dict(path='data/'+path.name,**st.digest(path)))
  if case=='bad-checksum':entries[0]['sha256']='b'*64
  value=dict(version=1,format='CSV',semanticsVersion='csv-v2',sourceSha256='a'*64,columns=['id','name'],rowCount=3 if case=='bad-row-count' else 2,files=entries);st.save_json(folder/'manifest.json',value)

def run(args):
 assert 120<=args.max_seconds<=1800
 root=args.output.absolute();root.resolve().relative_to(st.ROOT/'.runtime');assert not root.exists()
 old=args.baseline_run.resolve();old.relative_to(st.ROOT/'.runtime');assert (old/'COMPLETE').is_file()
 baseline=old/'app.jar';assert st.digest(baseline)['sha256']==args.baseline_sha256
 capacity=json.loads(args.host_capacity.read_text(encoding='utf-8-sig'));assert capacity.get('known') and capacity['freeBytes']>=8*1024**3 and shutil.disk_usage(root.parent).free>=8*1024**3
 allowed={st.Project().service(role) for role in ('mysql','backend','frontend')};assert set(st.run(['docker','ps','-q','--no-trunc']).decode().split()).issubset(allowed)
 with (st.ROOT/'.runtime/cluster-compare.lock').open('a') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);assert rt.active(st.Project())==0;before=b.source_snapshot();point=st.recovery_point(st.Project());root.mkdir(mode=0o700)
  st.save_json(root/'primary-before.json',before);st.save_json(root/'primary-point.json',point)
  (root/'inputs').mkdir(mode=0o755);(root/'output').mkdir(mode=0o755);os.chown(root/'output',185,0)
  shutil.copyfile(baseline,root/'baseline.jar');shutil.copyfile(st.ROOT/'spark-engine/target/spark-engine-0.1.0.jar',root/'candidate.jar');shutil.copyfile(root/'candidate.jar',root/'app.jar')
  shutil.copytree(old/'inputs/200000-csv',root/'inputs/200000-csv');shutil.copyfile(old/'inputs/200000.csv',root/'inputs/200000.csv');fixtures(root/'inputs')
  for path in (root/'inputs').rglob('*'):
   if path.is_file():os.chmod(path,0o444)
  inputs=f.fingerprint(root/'inputs');trials=plan(args.repeats);st.save_json(root/'plan.json',trials)
  sources={str(p.relative_to(st.ROOT)):st.digest(p) for p in (Path(__file__),st.ROOT/'spark-engine/src/main/java/cn/sparkdq/engine/PreparedInput.java',st.ROOT/'spark-engine/target/spark-engine-0.1.0.jar')}
  jars={v:st.digest(root/(v+'.jar')) for v in VARIANTS};assert jars['baseline']!=jars['candidate']
  cluster=Headers(root,uuid.uuid4().hex);cluster.seed=b.SEED;cluster.preparations={'200000/CSV':{'manifestSha256':st.digest(root/'inputs/200000-csv/manifest.json')['sha256']}}
  st.save_json(root/'environment.json',dict(session=cluster.session,image=f.c.s.IMAGE,jars=jars,baselineRun=str(old),hostCapacity=capacity,sourceDigests=sources,seed=b.SEED,inputPartitions=8,shufflePartitions=2,preparedFiles=4,aqe=False,pairedRandomizedOrder=True))
  records=[];deadline=time.monotonic()+args.max_seconds
  try:
   # No cluster daemons needed for tiny parser compatibility and signed negative fixtures.
   compatibility=cluster.compatibility();cluster.start_cluster()
   for trial in trials:
    assert time.monotonic()<deadline,'Group deadline exceeded';cluster.select_jar(trial['variant']);record=cluster.measure(trial,deadline)
    for peer in records:
     if peer['profile']==record['profile']:assert peer['metrics']==record['metrics'] and peer['anomalySha256']==record['anomalySha256']
    records.append(record);print(json.dumps({k:record[k] for k in ('index','variant','mode','profile','warmup','containerWallSeconds')}),flush=True)
  finally:cluster.close()
  assert before==b.source_snapshot() and point['sha256']==st.recovery_point(st.Project())['sha256'] and rt.active(st.Project())==0
  assert inputs==f.fingerprint(root/'inputs') and all(st.digest(st.ROOT/name)==value for name,value in sources.items()) and jars=={v:st.digest(root/(v+'.jar')) for v in VARIANTS}
  summary=dict(ok=True,groups=summarize(records),compatibilityCases=len(compatibility),measuredTrials=sum(not v['warmup'] for v in records),warmupTrials=sum(v['warmup'] for v in records),primaryUnchanged=True,allOwnedContainersStopped=True,networkRemoved=not cluster.network_created,scope='Same-host 200k prepared CSV, fresh JVMs, shared workload quotas 2CPU/5GiB + extra master; n=2 default descriptive, all observations retained, no production deployment or shared-cache claim')
  st.save_json(root/'measurements.json',records);st.save_json(root/'summary.json',summary);st.save_json(root/'COMPLETE',dict(summary=st.digest(root/'summary.json'),measurements=st.digest(root/'measurements.json'),compatibility=st.digest(root/'compatibility.json')));print(json.dumps(summary),flush=True)
def main():
 signal.signal(signal.SIGTERM,lambda *args:(_ for _ in ()).throw(InterruptedError('Bounded experiment interrupted')))
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--baseline-run',type=Path,required=True);p.add_argument('--baseline-sha256',required=True);p.add_argument('--host-capacity',type=Path,required=True);p.add_argument('--repeats',type=int,default=2);p.add_argument('--max-seconds',type=int,default=1800);run(p.parse_args())
if __name__=='__main__':main()
