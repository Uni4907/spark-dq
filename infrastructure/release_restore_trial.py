#!/usr/bin/env python3
from pathlib import Path
import sys,json,subprocess,time,traceback,re,shutil,hashlib,csv
P=Path('/home/sparkdq/projects/spark-dq');sys.path[:0]=[str(P/'infrastructure'),str(P/'tests')]
import storage as st,maintenance,release_lab as lab,t7_product_acceptance as million,mysql_import_trial as trial
from iqr_acceptance import Client
def stop_restore(root):
 receipt=root/'restored/receipt.json'
 if not receipt.exists():return []
 identity=json.loads(receipt.read_text())['project'];assert re.fullmatch('spark-dq-restore-[0-9a-f]{12}',identity);states=[]
 for cid in lab.out(['docker','ps','-aq','--filter','label=com.docker.compose.project='+identity]).split():
  node=json.loads(lab.out(['docker','inspect','--format','{{json .}}',cid]));assert node['Config']['Labels']['com.docker.compose.project']==identity
  if node['State']['Running']:subprocess.run(['docker','stop','--timeout','5',cid],check=True,stdout=subprocess.DEVNULL,timeout=25)
  states.append(dict(id=cid,stopped=lab.out(['docker','inspect','--format','{{.State.Running}}',cid])=='false'))
 return states
def main(root):
 root,meta,cmd=lab.context(root);m=json.loads(sorted(root.glob('million-acceptance-*/summary.json'))[-1].read_text());sources=json.loads(sorted(root.glob('sources-acceptance-*/summary.json'))[-1].read_text());hive=json.loads(sorted(root.glob('hive-acceptance-*/summary.json'))[-1].read_text());assert m['ok'] and sources['ok'] and hive['ok'];out=root/'recovery';out.mkdir(mode=0o700);project=st.Project(root/'compose.yaml',root/'.env');result=dict(ok=False)
 with (out/'guard.log').open('x') as log:guard=subprocess.Popen(['python3',str(Path(__file__).resolve()),'guard',str(root)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 lab.save(out/'guard-process.json',dict(pid=guard.pid,seconds=2100))
 try:
  capacity=dict(known=True,freeBytes=shutil.disk_usage('/mnt/c').free);lab.save(out/'host-capacity.json',capacity);maintenance.backup_once(project,out/'maintenance-backup',host_capacity=out/'host-capacity.json');manifest=st.verify(out/'maintenance-backup/backup');lab.stop(root);print('unified candidate consistent backup verified; source including business stopped',flush=True)
  receipt=st.restore(out/'maintenance-backup/backup',root/'restored');restored=st.Project(root/'restored/compose.yaml',root/'restored/.env');cid=restored.service('backend')
  for service,memory,cpu in [('backend','3g','2'),('mysql','768m','1')]:subprocess.run(['docker','update','--memory',memory,'--memory-swap',memory,'--cpus',cpu,restored.service(service)],check=True,stdout=subprocess.DEVNULL,timeout=30)
  control=maintenance.Control(restored);control.wait();c=Client(control.address());assert million.get(c,'/controlled-files')==[] and million.get(c,'/mysql-sources')==[]
  assert lab.out(['docker','exec',cid,'sha256sum','/app/engine/spark-engine-0.1.0.jar']).split()[0]==meta['candidate']['engineJarSha256'];backend_sha=lab.out(['docker','exec',cid,'sha256sum','/app/backend.jar']).split()[0];assert backend_sha==hashlib.sha256((P/'backend/target/backend-0.1.0.jar').read_bytes()).hexdigest()
  checks=[]
  for identity in [m['sourceDatasetId'],m['governedDatasetId'],hive['datasetId']]:
   data=million.get(c,'/datasets/'+identity);assert million.download(c,'/datasets/'+identity+'/download',out/(identity+'.csv'))==data['sha256'];assert million.download(c,'/datasets/'+identity+'/sources/1/download',out/(identity+'-source.csv'))==data['sha256'];checks.append(dict(datasetId=identity,rows=data['row_count'],sha256=data['sha256']))
  original=next(x['taskId'] for x in m['checks'] if x['name']=='governed version remains above20MiB and inherits controlled profile');task=c.task({},'/tasks/'+original+'/rerun');expected=json.loads((root/'governed-oracle/expected.json').read_text());million.verify(c,cid,task,expected,out/'restored-million-recheck');assert task['dataset_id']==m['governedDatasetId']
  with (root/'reference.csv').open(encoding='utf-8',newline='') as stream:hrows=[{k:None if v=='\\N' else v for k,v in r.items()} for r in csv.DictReader(stream)]
  replay=c.task({},'/tasks/'+hive['hiveTaskId']+'/rerun');trial.validate_task(c,cid,replay,hrows,out);assert million.get(c,'/datasets/'+hive['datasetId'])['source']['hive']==hive['source'];checks.append(dict(name='Hive original Catalog absent; stored source receipt/full metrics/multiset preserved',taskId=replay['id']))
  source_check=next(x for x in sources['checks'] if x['name'].startswith('unified candidate MySQL and CSV'));mysql_task=c.task({},'/tasks/'+source_check['mysqlTaskId']+'/rerun');trial.validate_task(c,cid,mysql_task,json.loads((root/'fixture-rows.json').read_text()),out);checks.append(dict(name='MySQL business server stopped and unconfigured; frozen snapshot recheck exact',taskId=mysql_task['id']))
  native=next(x for x in sources['checks'] if x['name'].startswith('native Parquet'));raw=c.raw('/datasets/'+native['datasetId']+'/sources/1/download');assert hashlib.sha256(raw).hexdigest()==hashlib.sha256((P/'tests/fixtures/t10-positive.parquet').read_bytes()).hexdigest();checks.append(dict(name='Native Parquet original source bytes preserved'))
  lab.save(out/'api-verification.json',dict(ok=True,checks=checks,restoredMillionTaskId=task['id'],backendJarSha256=backend_sha));subprocess.run(restored.compose+['stop','backend'],check=True,stdout=subprocess.DEVNULL,timeout=30);audit=st.audit(restored);assert audit['ok'];lab.save(out/'restored-audit.json',audit);result.update(ok=True,checks=checks,restoredMillionTaskId=task['id'],backendJarSha256=backend_sha,runtimeEngineJarSha256=meta['candidate']['engineJarSha256'],recoveryPoint=manifest['recoveryPoint']['sha256'],baseImageId=manifest['backendImageId'],sourceCandidateImageId=manifest['sourceContainerImageId'],restoredProject=receipt['project'],applicationIdentity='Restored /app hash verified read-only; base image alone is not application identity')
 except Exception:result['failure']=traceback.format_exc();raise
 finally:
  states=stop_restore(root);result['containers']=states;result['allRestoreContainersStopped']=all(n['stopped'] for n in states);lab.save(out/'summary.json',result);(root/'restore-guard-stop').touch();print(json.dumps(dict(ok=result['ok'],failure=result.get('failure'),recoveryPoint=result.get('recoveryPoint'))),flush=True)
if __name__=='__main__':
 if sys.argv[1]=='guard':
  root,meta,cmd=lab.context(sys.argv[2]);deadline=time.time()+2100
  while time.time()<deadline and not (root/'restore-guard-stop').exists():time.sleep(5)
  if not (root/'restore-guard-stop').exists():lab.save(root/'restore-auto-cleanup.json',dict(containers=stop_restore(root)))
 else:main(sys.argv[1])
