#!/usr/bin/env python3
"""Finite, isolated 50k/200k HTTP correctness and task-budget observation."""
import argparse,csv,hashlib,io,json,signal,sys,time
from pathlib import Path
import storage as st,benchmark as b,cluster_lab as lab,cluster_compare as c
import cluster_stability as soak,standalone_trial as remote,header_service_acceptance as service
sys.path.insert(0,str(st.ROOT/'tests'))
import e2e,execution_inputs as x
SIZES=(50000,200000)
BUDGET_MS=300000
def validate_options(sizes,maximum):
 if not isinstance(sizes,(list,tuple)) or not sizes or any(type(v) is not int or v not in SIZES for v in sizes) or len(set(sizes))!=len(sizes):raise ValueError('Distinct bounded sizes 50000 and/or 200000 required')
 if type(maximum) is not int or not 600<=maximum<=2400:raise ValueError('Finite 600..2400 second run required')
def budget_summary(records,sizes):
 expected={(rows,kind,action) for rows in sizes for kind in ('CSV','PARQUET') for action in ('detect','govern','recheck')}
 actual=[(v['sourceRows'],v['format'],v['action']) for v in records]
 if len(actual)!=len(expected) or set(actual)!=expected:raise ValueError('Missing or duplicate scale trial')
 if len({v['id'] for v in records})!=len(records):raise ValueError('Reused task identity')
 for item in records:
  if type(item['durationMs']) is not int or item['durationMs']<0:raise ValueError('Invalid persisted task duration')
  if type(item['inputBytes']) is not int or not 0<item['inputBytes']<=20*1024**2:raise ValueError('Upload bound exceeded')
  if item['totalRows']!=(item['sourceRows'] if item['action']=='detect' else item['sourceRows']//2):raise ValueError('Wrong task row count')
 maximum=max(v['durationMs'] for v in records)
 return {'configuredWorkerBudgetMs':BUDGET_MS,'maximumPersistedTaskDurationMs':maximum,'minimumConservativeMarginMs':BUDGET_MS-maximum,'allPersistedDurationsBelowBudget':maximum<BUDGET_MS,'interpretation':'persisted task duration includes preparation and result persistence; margin is an observed conservative indicator, not a timeout injection or future capacity guarantee'}
def run(args):
 validate_options(args.sizes,args.max_seconds);root=args.output.absolute();root.resolve().relative_to(st.ROOT/'.runtime');assert not root.exists()
 control,node,project=lab.load(args.lab);lab.owned(control,node,project);state=lab.status(control);assert state['alive'] and state['state']=='running';e2e.BASE=state['api'];assert e2e.http('/datasets/page')[1]['total']==0,'Fresh empty candidate required'
 assert project.volume('backend','/data')!=st.Project().volume('backend','/data');root.mkdir(mode=0o700);(root/'trials').mkdir();(root/'inputs').mkdir()
 before=b.source_snapshot();point=st.recovery_point(st.Project());st.save_json(root/'primary-before.json',before);st.save_json(root/'primary-point.json',point)
 primary_images={role:st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(role)]).decode().strip() for role in ('backend','frontend')};st.save_json(root/'primary-images.json',primary_images)
 ids={role:project.service(role) for role in soak.ROLES};initial={role:json.loads(st.run(['docker','inspect',cid]))[0] for role,cid in ids.items()};paths={role:c.cgroup(cid) for role,cid in ids.items()};sampler=soak.Sampler(paths)
 budget=st.run(['docker','exec',ids['backend'],'printenv','SPARK_DQ_TASK_TIMEOUT_SECONDS']).decode().strip() if any(value.startswith('SPARK_DQ_TASK_TIMEOUT_SECONDS=') for value in initial['backend']['Config']['Env']) else '300';assert budget=='300'
 jar=st.run(['docker','exec',ids['backend'],'sha256sum','/app/engine/spark-engine-0.1.0.jar']).decode().split()[0];assert jar==args.engine_sha256
 sources={str(p.relative_to(st.ROOT)):st.digest(p) for p in (Path(__file__),st.ROOT/'infrastructure/cluster_stability.py',st.ROOT/'infrastructure/header_service_acceptance.py',st.ROOT/'infrastructure/benchmark.py')}
 st.save_json(root/'environment.json',{'project':node['project'],'lab':str(control),'images':node['images'],'engineSha256':jar,'sizes':args.sizes,'budgetSeconds':300,'maximumSeconds':args.max_seconds,'sourceDigests':sources,'seed':b.SEED,'samplerIntervalSeconds':1})
 started=time.monotonic();deadline=started+args.max_seconds;own=[];records=[];idles=[];immutable={};reports={};versions={};inputs={};success=False
 def guard():
  sampler.check()
  if time.monotonic()>deadline:raise TimeoutError('Scale run deadline')
 def get(identity):
  code,task=e2e.http('/tasks/'+identity);assert code==200;return task
 def idle(label):
  guard();assert e2e.http('/execution/status')[1]=={'mode':'standalone','cleanupPending':0,'acceptingTasks':True}
  for status in ('QUEUED','RUNNING','CANCELLING'):assert e2e.http('/tasks/page?pageSize=1&status='+status)[1]['total']==0
  for role in soak.ROLES:
   raw=json.loads(st.run(['docker','inspect',ids[role]]))[0];assert raw['State']['Running'] and not raw['State']['OOMKilled'] and raw['State']['Pid']==initial[role]['State']['Pid'] and raw['RestartCount']==initial[role]['RestartCount']
  for role in ('worker1','worker2'):assert 'CoarseGrainedExecutorBackend' not in st.run(['docker','top',ids[role],'-eo','pid,args']).decode()
  process=st.run(['docker','top',ids['backend'],'-eo','pid,args']).decode();assert 'cn.sparkdq.engine.' not in process and len([v for v in process.splitlines()[1:] if 'java' in v])==1
  item={'label':label,'seconds':time.monotonic()-started,'activeTasks':0,'workerExecutors':0,'persistentDataBytes':int(st.run(['docker','exec',ids['backend'],'du','-sb','/data']).decode().split()[0])};idles.append(item);st.save_json(root/('idle-'+str(len(idles))+'.json'),item)
 def trial(path,body,rows,kind,action,input_bytes,expected,anomalies):
  guard();identity=x.call(path,body)['id'];own.append(identity);end=time.monotonic()+350
  while True:
   guard();task=get(identity)
   if task['status'] in ('SUCCEEDED','FAILED','CANCELLED'):break
   if time.monotonic()>end:raise TimeoutError('Task terminal/cleanup deadline')
   time.sleep(.3)
  folder=root/'trials'/str(len(records)+1);folder.mkdir();st.save_json(folder/'task.json',task)
  st.run(['docker','cp',ids['backend']+':/data/tasks/'+identity,str(folder/'result')],timeout=90)
  service.terminal(task,'SUCCEEDED');data=folder/'result';digests=soak.validate_full(task,data,expected,anomalies,action=='govern');remote.evidence(data/'executor-evidence.json',{'worker1','worker2'},2)
  assert task['execution']['evidenceState']=='DRIVER_ENDED' and task['execution']['ended']['driverOutcome']=='COMPLETED'
  prep=json.loads((data/'prepared/preparation.json').read_text());assert prep['verified'] and prep['manifestSha256']==task['snapshot']['input_manifest_sha256']
  result=json.loads((data/'result.json').read_text());assert result['execution']['inputFormat']==kind and result['execution']['inputPartitions']==8 and result['execution']['shufflePartitions']==2
  record={'id':identity,'sourceRows':rows,'inputBytes':input_bytes,'format':kind,'action':action,'totalRows':task['total_rows'],'durationMs':task['duration_ms'],'metrics':x.metrics(task),'anomalySha256':digests,'workerExecution':result['execution']};records.append(record);st.save_json(folder/'record.json',record);immutable[identity]=task
  if action=='govern':
   output=task['governance']['output_dataset_id'];body=x.download(output);assert b.row_digest(list(csv.reader(io.StringIO(body.decode(),newline='')))[1:])==expected['outputRowsSha256'];versions[output]=hashlib.sha256(body).hexdigest();reports[identity]=e2e.http('/tasks/'+identity+'/report')[1];st.save_json(folder/'report.json',reports[identity]);st.save_json(folder/'version.json',{'datasetId':output,'sha256':versions[output]})
  print(json.dumps({key:record[key] for key in ('id','sourceRows','format','action','totalRows','durationMs')}),flush=True);return task
 sampler.thread.start()
 try:
  idle('initial');rules=[{key:value for key,value in rule.items() if key!='id'} for rule in b.rules('extended')]
  for rows in args.sizes:
   path=root/'inputs'/('synthetic-'+str(rows)+'.csv');b.generate(path,rows);raw=path.read_bytes();code,dataset=e2e.upload(raw,path.name);assert code==200;inputs[dataset['id']]=hashlib.sha256(raw).hexdigest()
   expectations={profile:b.oracle(rows,profile) for profile in ('extended','governance')};anomalies={profile:soak.expected_anomalies(rows,profile) for profile in expectations}
   for kind in ('CSV','PARQUET'):
    detected=trial('/tasks',{'datasetId':dataset['id'],'rules':rules,'executionInput':{'format':kind,'partitions':4}},rows,kind,'detect',len(raw),expectations['extended'],anomalies['extended'])
    governed=trial('/tasks/'+detected['id']+'/govern',b.policy(),rows,kind,'govern',len(raw),expectations['governance'],anomalies['governance'])
    trial('/tasks/'+governed['id']+'/rerun',{},rows,kind,'recheck',len(raw),expectations['governance'],anomalies['governance']);idle(str(rows)+'-'+kind)
  for identity,expected in immutable.items():assert get(identity)==expected
  for identity,report in reports.items():assert e2e.http('/tasks/'+identity+'/report')[1]==report
  for identity,sha in {**versions,**inputs}.items():assert hashlib.sha256(x.download(identity)).hexdigest()==sha
  assert e2e.http('/tasks/page?pageSize=1')[1]['total']==len(records);idle('final');success=True
 finally:
  sampler.close();st.save_json(root/'resource-samples.json',{'samples':sampler.samples,'collectorError':sampler.error})
  if not success:
   for identity in own:
    try:
     if get(identity)['status'] in ('QUEUED','RUNNING','CANCELLING'):e2e.http('/tasks/'+identity+'/cancel',b'',method='POST')
    except (OSError,ValueError):pass
 assert before==b.source_snapshot() and point['sha256']==st.recovery_point(st.Project())['sha256']
 assert all(st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(role)]).decode().strip()==image for role,image in primary_images.items())
 assert all(st.digest(st.ROOT/name)==value for name,value in sources.items());sampler.check()
 summary={'ok':True,'elapsedSeconds':time.monotonic()-started,'sizes':args.sizes,'successfulHttpTasks':len(records),'successfulSparkApplications':len(records)*2,'fullAnomaliesAndGovernedMultisetsMatch':True,'historyReportsAndDownloadsImmutable':True,'idleChecks':len(idles),'noObservedRestartOomOrResidualExecutors':True,'primaryDataFilesImagesUnchanged':True,'budget':budget_summary(records,args.sizes),'resources':soak.summarize_resources(sampler.samples),'scope':'same-host two-worker sequential synthetic correctness/observed budget validation; not repeated performance comparison, large-scale production capacity, concurrency, or injected timeout proof'}
 st.save_json(root/'records.json',records);st.save_json(root/'summary.json',summary);st.save_json(root/'COMPLETE',{'summary':st.digest(root/'summary.json'),'records':st.digest(root/'records.json'),'resources':st.digest(root/'resource-samples.json')});print(json.dumps(summary),flush=True)
def main():
 signal.signal(signal.SIGTERM,lambda *unused:(_ for _ in ()).throw(InterruptedError('Scale validation interrupted')))
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--lab',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--engine-sha256',required=True);parser.add_argument('--sizes',nargs='+',type=int,default=list(SIZES));parser.add_argument('--max-seconds',type=int,default=2100);run(parser.parse_args())
if __name__=='__main__':main()
