#!/usr/bin/env python3
from pathlib import Path
import sys,json,subprocess,time,traceback,re,shutil
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'infrastructure'),str(ROOT/'tests')]
import storage as st,maintenance,t7_product_lab as lab,t7_product_acceptance as acceptance
from iqr_acceptance import Client
def stop_restore(root):
 receipt=root/'restored/receipt.json'
 if not receipt.exists():return []
 identity=json.loads(receipt.read_text())['project'];assert re.fullmatch('spark-dq-restore-[0-9a-f]{12}',identity);states=[]
 for cid in lab.output(['docker','ps','-aq','--filter','label=com.docker.compose.project='+identity]).split():
  node=json.loads(lab.output(['docker','inspect','--format','{{json .}}',cid]));assert node['Config']['Labels']['com.docker.compose.project']==identity
  if node['State']['Running']:subprocess.run(['docker','stop','--timeout','5',cid],check=True,stdout=subprocess.DEVNULL,timeout=25)
  states.append(dict(id=cid,stopped=lab.output(['docker','inspect','--format','{{.State.Running}}',cid])=='false'))
 return states
def main(root):
 root,meta,cmd=lab.context(root);source=sorted(root.glob('acceptance-*'))[-1];summary=json.loads((source/'summary.json').read_text());assert summary['ok'];out=root/'recovery';out.mkdir(mode=0o700);project=st.Project(root/'compose.yaml',root/'.env');result=dict(ok=False)
 guardlog=(out/'guard.log').open('x');guard=subprocess.Popen(['python3',str(Path(__file__).resolve()),'guard',str(root)],stdout=guardlog,stderr=subprocess.STDOUT,start_new_session=True);guardlog.close();lab.save(out/'guard-process.json',dict(pid=guard.pid,seconds=2100))
 try:
  capacity=dict(known=True,freeBytes=shutil.disk_usage('/mnt/c').free);lab.save(out/'host-capacity.json',capacity);maintenance.backup_once(project,out/'maintenance-backup',host_capacity=out/'host-capacity.json');manifest=st.verify(out/'maintenance-backup/backup');lab.stop(root);print('controlled datasets verified in consistent backup; source project stopped',flush=True)
  receipt=st.restore(out/'maintenance-backup/backup',root/'restored');restored=st.Project(root/'restored/compose.yaml',root/'restored/.env');cid=restored.service('backend');subprocess.run(['docker','update','--memory','3g','--memory-swap','3g','--cpus','2',cid],check=True,stdout=subprocess.DEVNULL,timeout=30);subprocess.run(['docker','update','--memory','768m','--memory-swap','768m','--cpus','1',restored.service('mysql')],check=True,stdout=subprocess.DEVNULL,timeout=30)
  control=maintenance.Control(restored);control.wait();c=Client(control.address());assert acceptance.get(c,'/controlled-files')==[];assert c.call('/controlled-files/million/register',None,'POST')[0]==400
  source_id=summary['sourceDatasetId'];version_id=summary['governedDatasetId'];assert acceptance.download(c,'/datasets/'+source_id+'/download',out/'restored-source.csv')==json.loads((root/'inputs.json').read_text())['sha256'];assert acceptance.download(c,'/datasets/'+version_id+'/sources/1/download',out/'restored-version.csv')==acceptance.get(c,'/datasets/'+version_id)['sha256']
  assert lab.output(['docker','exec',cid,'sha256sum','/app/engine/spark-engine-0.1.0.jar']).split()[0]==meta['candidate']['engineJarSha256']
  original=next(x['taskId'] for x in summary['checks'] if x['name']=='governed version remains above20MiB and inherits controlled profile');task=c.task({},'/tasks/'+original+'/rerun');expected=json.loads((root/'governed-oracle/expected.json').read_text());acceptance.verify(c,cid,task,expected,out/'restored-recheck');assert task['dataset_id']==version_id
  result.update(ok=True,sourceRows=1000000,versionRows=500000,originalSourceConfigured=False,downloadAndSourceFingerprintsVerified=True,fullMetricsAndAnomalyMultisetsVerified=True,restoredTaskId=task['id'],restoredProject=receipt['project'],runtimeEngineJarSha256=meta['candidate']['engineJarSha256'],recoveryPoint=manifest['recoveryPoint']['sha256'],baseImageId=manifest['backendImageId'],sourceCandidateImageId=manifest['sourceContainerImageId'],applicationIdentity='Restored /app is read-only and hash-verified; base image ID alone is not the application version')
 except Exception:result['failure']=traceback.format_exc();(out/'failure.txt').write_text(result['failure']);raise
 finally:
  states=stop_restore(root);result['allRestoreContainersStopped']=all(s['stopped'] for s in states);result['containers']=states;lab.save(out/'summary.json',result);(root/'restore-guard-stop').touch();print(json.dumps(result),flush=True)
if __name__=='__main__':
 if sys.argv[1]=='guard':
  root,meta,cmd=lab.context(sys.argv[2]);deadline=time.time()+2100
  while time.time()<deadline and not (root/'restore-guard-stop').exists():time.sleep(5)
  if not (root/'restore-guard-stop').exists():lab.save(root/'restore-auto-cleanup.json',dict(containers=stop_restore(root)))
 else:main(sys.argv[1])
