from pathlib import Path
import json,sys,subprocess,time,shutil,hashlib,collections,traceback,tarfile,re
P=Path('/home/sparkdq/projects/spark-dq');sys.path[:0]=[str(P/'infrastructure'),str(P/'tests')]
import demo,storage as st,maintenance,mysql_import_trial as trial
from iqr_acceptance import Client
root,node,project=demo.context();out=root/'final-recovery'
def stop_restore():
 f=out/'restored/receipt.json'
 if not f.exists():return
 identity=json.loads(f.read_text())['project'];assert re.fullmatch('spark-dq-restore-[a-f0-9]{12}',identity)
 ids=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project='+identity],text=True).split()
 for cid in ids:subprocess.run(['docker','stop','--timeout','5',cid],stdout=subprocess.DEVNULL,check=True,timeout=25)
if len(sys.argv)>1 and sys.argv[1]=='guard':
 deadline=time.time()+1200
 while time.time()<deadline and not (out/'guard.stop').exists():time.sleep(5)
 if not (out/'guard.stop').exists():stop_restore()
 sys.exit(0)
out.mkdir(mode=0o700);result=dict(ok=False)
with (out/'guard.log').open('x') as log:guard=subprocess.Popen(['python3',str(Path(__file__).resolve()),'guard'],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
try:
 capacity=out/'host-capacity.json';capacity.write_text(json.dumps(dict(known=True,freeBytes=shutil.disk_usage('/mnt/c').free)));maintenance.backup_once(project,out/'consistent-backup',host_capacity=capacity);manifest=st.verify(out/'consistent-backup/backup');receipt=st.restore(out/'consistent-backup/backup',out/'restored');restored=st.Project(out/'restored/compose.yaml',out/'restored/.env')
 for service,memory,cpu in [('backend','3g','2'),('mysql','768m','1')]:subprocess.run(['docker','update','--memory',memory,'--memory-swap',memory,'--cpus',cpu,restored.service(service)],stdout=subprocess.DEVNULL,check=True,timeout=20)
 control=maintenance.Control(restored);control.wait();c=Client(control.address());actual=subprocess.check_output(['docker','exec',restored.service('backend'),'sha256sum','/app/backend.jar'],text=True).split()[0];assert actual==node['backendJarSha256'];assert c.call('/mysql-sources')[1]==[] and c.call('/controlled-files')[1]==[]
 ds=json.loads((root/'demo-verification/summary.json').read_text());govern=ds['tasks']['governance'];old=c.call('/tasks/'+govern)[1];replay=c.task({},'/tasks/'+govern+'/rerun');actual_rows=trial.full_rows(restored.service('backend'),replay,out);old_rows={r['id']:[] for r in old['rules']}
 with tarfile.open(root/'demo-verification'/(govern+'-anomalies.tar')) as tar:
  for entry in tar:
   if entry.isfile() and Path(entry.name).name.startswith('part-') and entry.name.endswith('.json'):old_rows[Path(entry.name).parent.name]+=[json.loads(line) for line in tar.extractfile(entry) if line.strip()]
 for rule in replay['rules']:
  before=next(r for r in old['rules'] if trial.signature(r)==trial.signature(rule));assert before['metrics']==rule['metrics'] and trial.multiset(old_rows[before['id']])==trial.multiset(actual_rows[rule['id']])
 assert hashlib.sha256(c.raw('/datasets/'+ds['datasetId']+'/download')).hexdigest()==hashlib.sha256((P/'demo/orders.csv').read_bytes()).hexdigest();(out/'restored-replay.json').write_text(json.dumps(replay,ensure_ascii=False,indent=2));(out/'restored-report.html').write_bytes(c.raw('/tasks/'+replay['id']+'/report.html'));subprocess.run(restored.compose+['stop','backend'],stdout=subprocess.DEVNULL,check=True,timeout=30);audit=st.audit(restored);assert audit['ok'];result.update(ok=True,recoveryPoint=manifest['recoveryPoint']['sha256'],sourceCandidateImageId=manifest['sourceContainerImageId'],baseImageId=manifest['backendImageId'],backendJarSha256=actual,engineJarSha256=node['engineJarSha256'],allRulesAndCompleteAnomaliesPreserved=True,restoredTaskId=replay['id'],audit=audit,originalSourcesAbsent=True,stableDemoResumed=True)
except Exception:result['failure']=traceback.format_exc();raise
finally:
 stop_restore();result['restoredContainersStopped']=True;(out/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));(out/'guard.stop').touch();print(json.dumps(dict(ok=result['ok'],recoveryPoint=result.get('recoveryPoint'),failure=result.get('failure'))),flush=True)
