#!/usr/bin/env python3
"""Bounded candidate-only repeated HTTP lifecycle and resource observation; no main writes."""
import argparse,collections,csv,hashlib,io,json,signal,sys,threading,time,uuid
from pathlib import Path
import storage as st,benchmark as b,cluster_lab as lab,cluster_compare as c,standalone_trial as remote,header_service_acceptance as service
sys.path.insert(0,str(st.ROOT/'tests'))
import e2e,execution_inputs as x
ROLES=('backend','mysql','master','worker1','worker2','frontend')
def validate_options(rounds,rows,minimum,maximum):
 if type(rounds) is not int or not 1<=rounds<=4 or type(rows) is not int or not 1000<=rows<=20000 or rows%2:raise ValueError('1..4 rounds and even 1000..20000 rows required')
 if type(minimum) is not int or type(maximum) is not int or not 60<=minimum<maximum<=2400:raise ValueError('Finite observation interval required')
def sample(path):
 value=c.read_cgroup(path)
 if value is None:raise ValueError('Candidate cgroup disappeared')
 events=dict(line.split() for line in (path/'memory.events').read_text().splitlines());return dict(value,oom=int(events['oom']),oomKill=int(events['oom_kill']))
def summarize_resources(samples):
 if not samples:raise ValueError('Missing resource observations')
 roles=set(samples[0]['roles'])
 if not roles:raise ValueError('Missing resource roles')
 for before,after in zip(samples,samples[1:]):
  if set(after['roles'])!=roles:raise ValueError('Resource role disappeared')
  for role in roles:
   if after['roles'][role]['cpuUsec']<before['roles'][role]['cpuUsec']:raise ValueError('CPU counter reset')
   if any(after['roles'][role][key]>samples[0]['roles'][role][key] for key in ('oom','oomKill')):raise ValueError('Candidate OOM event')
 return {'sampleCount':len(samples),'sampledAggregateMemoryPeakBytes':max(sum(v['memoryBytes'] for v in item['roles'].values()) for item in samples),'roles':{role:{'sampledMemoryPeakBytes':max(item['roles'][role]['memoryBytes'] for item in samples),'sampledCpuSeconds':(samples[-1]['roles'][role]['cpuUsec']-samples[0]['roles'][role]['cpuUsec'])/1e6,'oomDelta':samples[-1]['roles'][role]['oom']-samples[0]['roles'][role]['oom'],'oomKillDelta':samples[-1]['roles'][role]['oomKill']-samples[0]['roles'][role]['oomKill']} for role in sorted(roles)}}
class Sampler:
 def __init__(self,paths):self.paths=paths;self.samples=[];self.error=None;self.stop_event=threading.Event();self.started=time.monotonic();self.thread=threading.Thread(target=self.collect,daemon=True)
 def collect(self):
  try:
   while not self.stop_event.is_set():
    if len(self.samples)>=3000:raise ValueError('Resource sample budget exceeded')
    self.samples.append({'seconds':time.monotonic()-self.started,'roles':{role:sample(path) for role,path in self.paths.items()}})
    if len(self.samples)>1:summarize_resources(self.samples[-2:])
    self.stop_event.wait(1)
  except BaseException as error:self.error=repr(error);self.stop_event.set()
 def check(self):
  if self.error:raise RuntimeError(self.error)
 def close(self):self.stop_event.set();self.thread.join(timeout=3);assert not self.thread.is_alive()

def expected_anomalies(rows,profile):
 data=list(b.governed_rows(rows,b.SEED)) if profile=='governance' else list(b.data_rows(rows,b.SEED));groups=collections.Counter(row[0] for row in data);out={rule['id']:[] for rule in b.rules('extended')}
 for row in data:
  logical=[None if value=='\\N' else value for value in row]
  for index,name in ((1,'name'),(2,'email')):
   if b.missing(row[index]):out['missing-'+name].append(logical)
  if groups[row[0]]>1:out['duplicate-id'].append(logical+[groups[row[0]]])
  try:amount=int(row[3]);invalid=False
  except ValueError:amount=0;invalid=True
  if invalid:out['type-amount'].append(logical)
  if invalid or not 0<=amount<=500:out['range-amount'].append(logical)
  if row[6]!='ACTIVE':out['regex-status'].append(logical)
 return {key:b.row_digest(value) for key,value in out.items()}
def validate_full(task,folder,expected,anomaly_expected,govern=False):
 result=json.loads((folder/'result.json').read_text());service.check_counts(task,result,expected);assert result['totalRows']==expected['totalRows']
 definitions={(rule['type'],tuple(rule['columns'])):rule['id'] for rule in b.rules('extended')};keys={rule['id']:definitions[rule['type'],tuple(rule['columns'])] for rule in task['rules']};digests={}
 for rule in result['results']:
  logical=keys[rule['ruleId']];values=[json.loads(line) for part in (folder/'anomalies'/rule['ruleId']).glob('part-*.json') for line in part.read_text().splitlines()];metrics=rule['metrics'];assert len(values)==metrics.get('blankCount',metrics.get('duplicateRows',metrics.get('invalidCount')))
  assert all(set(row)==set(b.COLUMNS)|({'__dq_group_size'} if logical=='duplicate-id' else set()) for row in values)
  digests[logical]=b.row_digest([[row.get(name) for name in b.COLUMNS]+([row['__dq_group_size']] if logical=='duplicate-id' else []) for row in values])
 assert digests==anomaly_expected,'Full anomaly multiset differs from independent expected rows'
 if govern:
  assert result['governance']==expected['audit'];parts=list((folder/'governed').glob('part-*.csv'));assert len(parts)==1
  with parts[0].open(newline='',encoding='utf-8') as stream:reader=csv.reader(stream);assert next(reader)==b.COLUMNS;assert b.row_digest(reader)==expected['outputRowsSha256']
 return digests

def run(args):
 validate_options(args.rounds,args.rows,args.min_seconds,args.max_seconds);root=args.output.absolute();root.resolve().relative_to(st.ROOT/'.runtime');assert not root.exists()
 control,node,project=lab.load(args.lab);lab.owned(control,node,project);state=lab.status(control);assert state['alive'] and state['state']=='running';e2e.BASE=state['api'];assert e2e.http('/datasets/page')[1]['total']==0,'Fresh empty candidate lab required'
 assert project.volume('backend','/data')!=st.Project().volume('backend','/data');root.mkdir(mode=0o700);(root/'trials').mkdir()
 before=b.source_snapshot();point=st.recovery_point(st.Project());st.save_json(root/'primary-before.json',before);st.save_json(root/'primary-point.json',point)
 primary_images={role:st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(role)]).decode().strip() for role in ('backend','frontend')};st.save_json(root/'primary-images.json',primary_images)
 ids={role:project.service(role) for role in ROLES};initial={role:json.loads(st.run(['docker','inspect',cid]))[0] for role,cid in ids.items()};paths={role:c.cgroup(cid) for role,cid in ids.items()};sampler=Sampler(paths);deadline=time.monotonic()+args.max_seconds;started=time.monotonic();own=[];records=[];idles=[];immutable={};reports={};versions={};success=False
 sources={str(p.relative_to(st.ROOT)):st.digest(p) for p in (Path(__file__),st.ROOT/'infrastructure/header_service_acceptance.py',st.ROOT/'infrastructure/benchmark.py')}
 jar=st.run(['docker','exec',ids['backend'],'sha256sum','/app/engine/spark-engine-0.1.0.jar']).decode().split()[0];assert jar==args.engine_sha256
 st.save_json(root/'environment.json',{'project':node['project'],'lab':str(control),'images':node['images'],'engineSha256':jar,'rounds':args.rounds,'rows':args.rows,'minimumSeconds':args.min_seconds,'maximumSeconds':args.max_seconds,'sourceDigests':sources,'seed':b.SEED,'samplerIntervalSeconds':1})
 def guard():
  sampler.check()
  if time.monotonic()>deadline:raise TimeoutError('Bounded stability deadline')
 def get(identity):
  code,task=e2e.http('/tasks/'+identity);assert code==200;return task
 def submit(path,body):
  guard();task=x.call(path,body);own.append(task['id']);return task['id']
 def wait(identity,wanted='SUCCEEDED',receipt=False):
  end=time.monotonic()+(150 if receipt else 330)
  while time.monotonic()<end:
   guard();task=get(identity)
   if receipt and task['status']=='RUNNING' and task['execution']['evidenceState']=='STARTED':return task
   if not receipt and task['status'] in ('SUCCEEDED','FAILED','CANCELLED'):
    service.terminal(task,wanted);return task
   assert task['status'] not in ('FAILED','CANCELLED','SUCCEEDED'),task;time.sleep(.2)
  raise TimeoutError('Task lifecycle deadline')
 def idle(label):
  guard();assert e2e.http('/execution/status')[1]=={'mode':'standalone','cleanupPending':0,'acceptingTasks':True}
  for status in ('QUEUED','RUNNING','CANCELLING'):assert e2e.http('/tasks/page?pageSize=1&status='+status)[1]['total']==0
  for role in ROLES:
   raw=json.loads(st.run(['docker','inspect',ids[role]]))[0];assert raw['State']['Running'] and not raw['State']['OOMKilled'] and raw['State']['Pid']==initial[role]['State']['Pid'] and raw['RestartCount']==initial[role]['RestartCount']
  for role in ('worker1','worker2'):assert 'CoarseGrainedExecutorBackend' not in st.run(['docker','top',ids[role],'-eo','pid,args']).decode()
  processes=st.run(['docker','top',ids['backend'],'-eo','pid,args']).decode();assert 'cn.sparkdq.engine.' not in processes and len([line for line in processes.splitlines()[1:] if 'java' in line])==1,processes
  item={'label':label,'seconds':time.monotonic()-started,'resources':{role:sample(path) for role,path in paths.items()},'persistentDataBytes':int(st.run(['docker','exec',ids['backend'],'du','-sb','/data']).decode().split()[0]),'activeTasks':0,'workerExecutors':0};idles.append(item);st.save_json(root/('idle-'+str(len(idles))+'.json'),item)
 def prove(identity,profile,action,round_no,kind):
  task=wait(identity);folder=root/'trials'/str(len(records)+1);folder.mkdir();st.run(['docker','cp',ids['backend']+':/data/tasks/'+identity,str(folder/'result')],timeout=60);data=folder/'result';expected=b.oracle(args.rows,profile);digests=validate_full(task,data,expected,expected_anomalies(args.rows,profile),action=='govern')
  remote.evidence(data/'executor-evidence.json',{'worker1','worker2'},2);assert task['execution']['evidenceState']=='DRIVER_ENDED' and task['execution']['ended']['driverOutcome']=='COMPLETED'
  prep=json.loads((data/'prepared/preparation.json').read_text());assert prep['verified'] and prep['manifestSha256']==task['snapshot']['input_manifest_sha256']
  result=json.loads((data/'result.json').read_text());assert result['execution']['inputFormat']==kind and result['execution']['inputPartitions']==8 and result['execution']['shufflePartitions']==2
  record={'id':identity,'round':round_no,'format':kind,'action':action,'profile':profile,'totalRows':task['total_rows'],'durationMs':task['duration_ms'],'metrics':x.metrics(task),'anomalySha256':digests};records.append(record);st.save_json(folder/'task.json',task);st.save_json(folder/'record.json',record);immutable[identity]=task
  if action=='govern':
   output=task['governance']['output_dataset_id'];body=x.download(output);assert b.row_digest(list(csv.reader(io.StringIO(body.decode(),newline='')))[1:])==expected['outputRowsSha256'];versions[output]=hashlib.sha256(body).hexdigest();reports[identity]=e2e.http('/tasks/'+identity+'/report')[1];st.save_json(folder/'report.json',reports[identity]);st.save_json(folder/'version.json',{'datasetId':output,'sha256':hashlib.sha256(body).hexdigest()})
  print(json.dumps({'round':round_no,'format':kind,'action':action,'rows':task['total_rows'],'taskId':identity}),flush=True);return task
 sampler.thread.start()
 try:
  idle('initial');b.generate(root/'input.csv',args.rows);raw=(root/'input.csv').read_bytes();code,dataset=e2e.upload(raw,'stability-synthetic.csv');assert code==200
  rules=[{key:value for key,value in rule.items() if key!='id'} for rule in b.rules('extended')]
  def request(kind):return {'datasetId':dataset['id'],'rules':rules,'executionInput':{'format':kind,'partitions':4}}
  for round_no in range(1,args.rounds+1):
   for kind in (('CSV','PARQUET') if round_no%2 else ('PARQUET','CSV')):
    detected=prove(submit('/tasks',request(kind)),'extended','detect',round_no,kind)
    governed=prove(submit('/tasks/'+detected['id']+'/govern',b.policy()),'governance','govern',round_no,kind)
    prove(submit('/tasks/'+governed['id']+'/rerun',{}),'governance','recheck',round_no,kind);idle('round-'+str(round_no)+'-'+kind)
   for identity,expected in immutable.items():assert get(identity)==expected
   for identity,report in reports.items():assert e2e.http('/tasks/'+identity+'/report')[1]==report
  # Single-slot scheduler: cancellation of a queued task must never create an Engine receipt.
  blocker=submit('/tasks',request('CSV'));end=time.monotonic()+40
  while get(blocker)['status']!='RUNNING':guard();assert time.monotonic()<end;time.sleep(.1)
  queued=submit('/tasks',request('CSV'));assert get(queued)['status']=='QUEUED';assert e2e.http('/tasks/'+queued+'/cancel',b'',method='POST')[0]==200
  cancelled=get(queued);assert cancelled['status']=='CANCELLED' and cancelled['execution']['evidenceState']=='ABSENT' and all(row['metrics'] is None for row in cancelled['rules']);st.save_json(root/'queued-cancelled.json',cancelled)
  prove(blocker,'extended','queue-blocker',0,'CSV');assert get(queued)==cancelled;idle('queued-cancelled')
  running=submit('/tasks',request('CSV'));begun=wait(running,receipt=True);st.save_json(root/'running-before-cancel.json',begun);assert e2e.http('/tasks/'+running+'/cancel',b'',method='POST')[0]==200
  stopped=wait(running,'CANCELLED');assert stopped['execution']['started']['applicationId']==begun['execution']['started']['applicationId'];st.save_json(root/'running-cancelled.json',stopped);idle('running-cancelled')
  prove(submit('/tasks/'+running+'/rerun',{}),'extended','cancel-replay',0,'CSV');idle('replay-complete')
  while time.monotonic()-started<args.min_seconds:guard();sampler.stop_event.wait(min(5,args.min_seconds-(time.monotonic()-started)))
  for identity,expected in immutable.items():assert get(identity)==expected
  for identity,report in reports.items():assert e2e.http('/tasks/'+identity+'/report')[1]==report
  assert x.download(dataset['id'])==raw
  for identity,sha in versions.items():assert hashlib.sha256(x.download(identity)).hexdigest()==sha
  idle('final');success=True
 finally:
  sampler.close();st.save_json(root/'resource-samples.json',{'samples':sampler.samples,'collectorError':sampler.error})
  if not success:
   for identity in own:
    try:
     if get(identity)['status'] in ('QUEUED','RUNNING','CANCELLING'):e2e.http('/tasks/'+identity+'/cancel',b'',method='POST')
    except (OSError,ValueError):pass
 assert before==b.source_snapshot() and point['sha256']==st.recovery_point(st.Project())['sha256']
 assert all(st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(role)]).decode().strip()==image for role,image in primary_images.items())
 assert all(st.digest(st.ROOT/name)==value for name,value in sources.items());resources=summarize_resources(sampler.samples)
 summary={'ok':True,'elapsedSeconds':time.monotonic()-started,'rounds':args.rounds,'rows':args.rows,'successfulHttpTasks':len(records),'queuedCancelled':1,'runningCancelled':1,'allFullAnomaliesMatchIndependentRows':True,'allGovernedMultisetsMatchIndependentRows':True,'historyAndReportJsonImmutable':True,'noObservedServiceRestartOrOom':True,'idleChecks':len(idles),'allIdleChecksNoExecutorsOrEngineChildren':True,'resources':resources,'primaryDataFilesImagesUnchanged':True,'scope':'bounded same-host repeated synthetic tasks, not multi-day leak proof, load capacity, HA, or performance comparison; memory.current includes file cache; retained task output growth is expected'}
 st.save_json(root/'records.json',records);st.save_json(root/'summary.json',summary);st.save_json(root/'COMPLETE',{'summary':st.digest(root/'summary.json'),'records':st.digest(root/'records.json'),'resources':st.digest(root/'resource-samples.json')});print(json.dumps(summary),flush=True)
def main():
 signal.signal(signal.SIGTERM,lambda *unused:(_ for _ in ()).throw(InterruptedError('Bounded observation interrupted')))
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--lab',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--engine-sha256',required=True);parser.add_argument('--rounds',type=int,default=3);parser.add_argument('--rows',type=int,default=10000);parser.add_argument('--min-seconds',type=int,default=900);parser.add_argument('--max-seconds',type=int,default=1800);run(parser.parse_args())
if __name__=='__main__':main()
