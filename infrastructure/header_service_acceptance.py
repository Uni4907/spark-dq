#!/usr/bin/env python3
"""Candidate lab HTTP acceptance: immutable copies, versions/reports, restart and interrupted remote recovery."""
import argparse,json,os,shutil,subprocess,sys,time,uuid
from pathlib import Path
import storage as st,benchmark as b,cluster_lab as lab,standalone_trial as remote
sys.path.insert(0,str(st.ROOT/'tests'))
import e2e,execution_inputs as x

def terminal(task,status):
 assert task['status']==status,task
 assert task['clusterExecution']['cleanupObservation']=='CLEAR',task
 if status!='SUCCEEDED':assert all(row.get('metrics') is None for row in task.get('rules',[])),task
 return task

def check_counts(task,result,expected):
 definitions={(row['type'],tuple(row['columns'])):expected['metrics'][row['id']] for row in b.rules('extended')}
 actual={(row['type'],tuple(row['columns'])):row['metrics'] for row in task['rules']}
 assert actual==definitions,'Independent 10k metrics mismatch'
 by_id={row['id']:definitions[row['type'],tuple(row['columns'])] for row in task['rules']}
 assert {row['ruleId']:row['metrics'] for row in result['results']}==by_id,'Worker/persisted metrics mismatch'

def run(args):
 output=args.output.absolute();output.resolve().relative_to(st.ROOT/'.runtime');assert not output.exists()
 control,node,project=lab.load(args.lab);assert control!=st.ROOT/'.runtime/cluster-ui-20261020-b' and project.volume('backend','/data')!=st.Project().volume('backend','/data')
 state=lab.status(control);assert state['alive'] and state['state']=='running';lab.owned(control,node,project)
 e2e.BASE=state['api'];assert e2e.http('/datasets/page')[1]['total']==0,'Acceptance requires a fresh empty candidate lab'
 assert e2e.http('/execution/status')[1]=={'mode':'standalone','cleanupPending':0,'acceptingTasks':True}
 output.mkdir(mode=0o700);before=b.source_snapshot();point=st.recovery_point(st.Project());st.save_json(output/'primary-before.json',before);st.save_json(output/'primary-point.json',point)
 images={role:st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(role)]).decode().strip() for role in ('backend','frontend')};st.save_json(output/'primary-images.json',images)
 candidate=project.service('backend');jar=st.run(['docker','exec',candidate,'sha256sum','/app/engine/spark-engine-0.1.0.jar']).decode().split()[0]
 assert jar==args.engine_sha256==st.digest(st.ROOT/'spark-engine/target/spark-engine-0.1.0.jar')['sha256'];assert st.run(['docker','inspect','--format','{{.Image}}',candidate]).decode().strip()==node['images']['backend']
 st.save_json(output/'environment.json',{'lab':str(control),'project':node['project'],'images':node['images'],'engineSha256':jar,'sourceDigests':{str(p.relative_to(st.ROOT)):st.digest(p) for p in (Path(__file__),st.ROOT/'infrastructure/cluster_lab.py',st.ROOT/'tests/execution_inputs.py')}})
 def owned_backend():lab.owned(control,node,project);assert project.service('backend')==candidate
 def healthy():
  e2e.BASE=lab.address(project,'backend',8080)+'/api'
  end=time.monotonic()+110
  while time.monotonic()<end:
   try:
    if e2e.http('/health')[1]['status']=='UP':return
   except (OSError,ValueError):pass
   time.sleep(.4)
  raise AssertionError('Candidate health deadline')
 def wait(identity,status='SUCCEEDED'):
  end=time.monotonic()+330
  while time.monotonic()<end:
   code,t=e2e.http('/tasks/'+identity);assert code==200
   if t['status'] in ('SUCCEEDED','FAILED','CANCELLED'):return terminal(t,status)
   time.sleep(.2)
  raise AssertionError('Task terminal/cleanup deadline')
 def started(identity):
  end=time.monotonic()+120
  while time.monotonic()<end:
   task=e2e.http('/tasks/'+identity)[1]
   if task['status']=='RUNNING' and task['execution']['evidenceState']=='STARTED':return task
   assert task['status'] not in ('SUCCEEDED','FAILED','CANCELLED'),task
   time.sleep(.08)
  raise AssertionError('Remote application identity deadline')
 def json_file(identity,name):
  assert str(uuid.UUID(identity))==identity and name in ('executor-evidence.json','result.json','prepared/manifest.json','prepared/preparation.json')
  return json.loads(st.run(['docker','exec',candidate,'cat','/data/tasks/'+identity+'/'+name]))
 def proof(task,minimum=1):
  terminal(task,'SUCCEEDED');p=json_file(task['id'],'executor-evidence.json');path=output/('executors-'+task['id']+'.json');st.save_json(path,p);remote.evidence(path,{'worker1','worker2'},minimum)
  result=json_file(task['id'],'result.json');assert result['execution']['sourcePartitions']>0 and result['execution']['inputPartitions']==8 and result['execution']['shufflePartitions']==2
  assert task['execution']['evidenceState']=='DRIVER_ENDED' and task['execution']['ended']['driverOutcome']=='COMPLETED'
  st.save_json(output/('worker-result-'+task['id']+'.json'),result)
  if task['snapshot'].get('executionInput'):
   manifest=json_file(task['id'],'prepared/manifest.json');st.save_json(output/('manifest-'+task['id']+'.json'),manifest)
   prep=json_file(task['id'],'prepared/preparation.json');assert prep['verified'];assert prep['manifestSha256']==task['snapshot']['input_manifest_sha256']
  return result
 with (output/'execution-inputs.log').open('x') as stream:
  done=subprocess.run([sys.executable,str(st.ROOT/'tests/execution_inputs.py'),'--base',e2e.BASE],stdout=stream,stderr=subprocess.STDOUT,cwd=st.ROOT,timeout=1100)
 assert done.returncode==0,'See execution-inputs.log'
 info=json.loads((output/'execution-inputs.log').read_text().splitlines()[-1]);fixture=json.loads(Path(info['fixture']).read_text());assert len(fixture['tasks'])==8
 st.save_json(output/'fixture.json',fixture)
 for task in fixture['tasks']:proof(task)
 x.check(fixture);lab_point=st.recovery_point(project);st.save_json(output/'lab-before-restart.json',lab_point)
 print('PASS 8 HTTP tasks: original/CSV/Parquet detection, governance, inherited recheck, single-CSV reset; immutable report/version bytes',flush=True)
 # Idle whole-stack stop/start preserves all candidate volumes; no existing demo or primary restart.
 owned_backend();assert lab.rt.active(project)==0;st.run(project.compose+['stop'],timeout=120);st.run(project.compose+['up','-d','--no-build','--pull','never'],timeout=150)
 e2e.BASE=lab.address(project,'backend',8080)+'/api';healthy();x.check(fixture)
 assert lab_point['sha256']==st.recovery_point(project)['sha256'];st.save_json(output/'restart-verified.json',{'pointSha256':lab_point['sha256'],'immutableFixtureChecked':True});print('PASS whole-stack restart: tasks/snapshots/plans/metrics/report/download bytes and recovery point identical',flush=True)
 # Independent 10k oracle and actual tasks on both workers.
 b.generate(output/'sample.csv',10000);code,data=e2e.upload((output/'sample.csv').read_bytes(),'candidate-header-10000.csv');assert code==200
 rules=[{key:value for key,value in rule.items() if key!='id'} for rule in b.rules('extended')];request={'datasetId':data['id'],'rules':rules,'executionInput':{'format':'CSV','partitions':4}}
 task=wait(x.call('/tasks',request)['id']);result=proof(task,2);expected=b.oracle(10000,'extended');assert result['totalRows']==10000
 check_counts(task,result,expected)
 st.save_json(output/'large-task.json',task);print('PASS 10k prepared CSV HTTP task: independent 7-rule counts and both workers completed real tasks',flush=True)
 # Kill only this fresh candidate backend after remote Engine STARTED; do not infer cleanup from process exit.
 crash=x.call('/tasks',request)['id'];crash_started=started(crash);st.save_json(output/'interrupted-started.json',crash_started)
 owned_backend();st.run(['docker','kill','--signal','KILL',candidate],timeout=20);st.run(['docker','start',candidate],timeout=30);healthy()
 failed=wait(crash,'FAILED');assert '重启' in failed['error'];assert failed['execution']['started']['applicationId']==crash_started['execution']['started']['applicationId']
 assert all('CoarseGrainedExecutorBackend' not in st.run(['docker','top',project.service(role),'-eo','pid,args']).decode() for role in ('worker1','worker2'))
 assert e2e.http('/execution/status')[1]=={'mode':'standalone','cleanupPending':0,'acceptingTasks':True}
 st.save_json(output/'interrupted-recovered.json',failed);print('PASS interrupted remote task: FAILED only after CLEAR, no partial result metrics, preserved application identity, no worker executors',flush=True)
 replay=wait(x.call('/tasks/'+crash+'/rerun',{})['id']);proof(replay,2);assert x.metrics(replay)==x.metrics(task);st.save_json(output/'replayed-task.json',replay);x.check(fixture)
 assert before==b.source_snapshot() and point['sha256']==st.recovery_point(st.Project())['sha256']
 assert all(st.run(['docker','inspect','--format','{{.Image}}',st.Project().service(role)]).decode().strip()==image for role,image in images.items())
 summary={'ok':True,'candidateProject':node['project'],'engineSha256':jar,'successfulHttpTasks':10,'interruptedFailedTasks':1,'eightTaskFixtureSurvivesWholeStackRestart':True,'reportAndVersionBytesIdentical':True,'bothWorkersOn10kAndReplay':True,'recoveryGateClearAndAdmissionReopened':True,'primaryDataFilesImagesUnchanged':True,'primaryPointSha256':point['sha256'],'scope':'new isolated same-host candidate only; no production replacement, no performance remeasurement, no cross-host/HA claim'}
 st.save_json(output/'summary.json',summary);st.save_json(output/'COMPLETE',{'summary':st.digest(output/'summary.json'),'fixture':st.digest(output/'fixture.json')});print(json.dumps(summary),flush=True)
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--lab',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--engine-sha256',required=True);run(p.parse_args())
if __name__=='__main__':main()
