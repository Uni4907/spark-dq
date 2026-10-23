from pathlib import Path
import subprocess,json,sys,time,secrets,shutil,hashlib,traceback,re,urllib.request
P=Path('/home/sparkdq/projects/spark-dq');sys.path[:0]=[str(P/'infrastructure'),str(P/'tests')]
import storage as st,cluster_lab,algorithm_experiments as ex
from iqr_acceptance import Client
def save(path,obj):path.write_text(json.dumps(obj,indent=2))
def output(args):return subprocess.check_output(args,text=True,timeout=60).strip()
def stop(root):
 states=[]
 for project in json.loads((root/'projects.json').read_text()):
  assert re.fullmatch('sparkdq-t10-rehearsal-[a-f0-9]{12}',project)
  for cid in output(['docker','ps','-aq','--filter','label=com.docker.compose.project='+project]).split():
   node=json.loads(output(['docker','inspect','--format','{{json .}}',cid]));assert node['Config']['Labels']['com.docker.compose.project']==project
   if node['State']['Running']:subprocess.run(['docker','stop','--timeout','5',cid],check=True,stdout=subprocess.DEVNULL,timeout=25)
   states.append(dict(id=cid,stopped=output(['docker','inspect','--format','{{.State.Running}}',cid])=='false'))
 return states
if len(sys.argv)>1 and sys.argv[1]=='guard':
 root=Path(sys.argv[2]);deadline=time.time()+1200
 while time.time()<deadline and not (root/'guard.stop').exists():time.sleep(5)
 if not (root/'guard.stop').exists():save(root/'auto-cleanup.json',dict(containers=stop(root)))
 sys.exit(0)
cold=sorted((P/'.runtime').glob('t10-main-cold-*'))[-1];original=json.loads((cold/'receipt.json').read_text());assert original['ok'];root=P/'.runtime'/('t10-main-rehearsal-'+time.strftime('%Y%m%d-%H%M%S'));root.mkdir(mode=0o700);save(root/'projects.json',[])
with (root/'guard.log').open('x') as log:guard=subprocess.Popen(['python3',str(Path(__file__).resolve()),'guard',str(root)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
save(root/'guard.json',dict(pid=guard.pid,seconds=1200));candidate=json.loads(sorted((P/'.runtime').glob('t10-build-*/candidate.json'))[-1].read_text());images={n['name'].split('-')[-2]:n['imageId'] for n in original['containers']};result=dict(ok=False,coldBackup=str(cold),phases={});base='maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d'
try:
 assert ex.primary_fingerprints()==original['sourceFingerprints']
 for phase in ['rollback-old','upgrade-candidate']:
  work=root/phase;work.mkdir(mode=0o700);project='sparkdq-t10-rehearsal-'+secrets.token_hex(6);projects=json.loads((root/'projects.json').read_text());projects.append(project);save(root/'projects.json',projects)
  assert int(next(l.split()[1] for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith('MemAvailable:')))*1024>4*1024**3
  for name,archive in [('mysql-data','mysql-volume.tar.gz'),('files','files-volume.tar.gz')]:
   f=cold/archive;assert hashlib.sha256(f.read_bytes()).hexdigest()==original['files'][archive]['sha256'];volume=project+'_'+name;assert subprocess.run(['docker','volume','inspect',volume],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode!=0
   subprocess.run(['docker','volume','create','--label','com.docker.compose.project='+project,volume],stdout=subprocess.DEVNULL,check=True,timeout=20)
   subprocess.run(['docker','run','--rm','--network','none','--memory','512m','--cpus','1','-v',str(cold)+':/from:ro','-v',volume+':/to','--entrypoint','tar',base,'--numeric-owner','-xzpf','/from/'+archive,'-C','/to'],check=True,timeout=120,stdout=subprocess.DEVNULL)
  shutil.copyfile(cold/'private.env',work/'private.env');(work/'private.env').chmod(0o600)
  backend=images['backend'] if phase=='rollback-old' else candidate['backendImageId'];frontend=images['frontend'] if phase=='rollback-old' else candidate['frontendImageId'];text=(P/'infrastructure/compose.yaml').read_text().replace('name: spark-dq','name: '+project).replace('spark-dq-backend:0.1.0',backend).replace('spark-dq-frontend:0.1.0',frontend)
  for role in ['backend','frontend']:text=text.replace('    build:\n      context: ..\n      dockerfile: infrastructure/'+role+'.Dockerfile\n','')
  text=text.replace('127.0.0.1:18080:8080','127.0.0.1::8080').replace('127.0.0.1:15173:5173','127.0.0.1::5173').replace('\n  backend:\n','\n  backend:\n    cpus: 2\n    mem_limit: 3g\n').replace('\n  mysql:\n','\n  mysql:\n    cpus: 1\n    mem_limit: 768m\n').replace('\n  frontend:\n','\n  frontend:\n    mem_limit: 256m\n');plan=cluster_lab.network_plan();text+='\nnetworks:\n  default:\n    ipam:\n      config:\n        - subnet: '+plan['default']+'\n';(work/'compose.yaml').write_text(text)
  project_object=st.Project(work/'compose.yaml',work/'private.env');cmd=project_object.compose
  with (work/'startup.log').open('x') as log:subprocess.run(cmd+['up','-d','--no-build','--pull','never'],check=True,stdout=log,stderr=subprocess.STDOUT,timeout=180)
  c=Client('http://'+output(cmd+['port','backend','8080'])+'/api');deadline=time.monotonic()+70
  while True:
   try:
    code,health=c.call('/health');assert code==200 and health['status']=='UP';break
   except Exception:assert time.monotonic()<deadline;time.sleep(1)
  tables=project_object.mysql('SHOW TABLES;').decode().splitlines();counts={t:int(project_object.mysql('SELECT COUNT(*) FROM '+t+';').strip()) for t in tables};datasets=project_object.rows("SELECT JSON_OBJECT('id',id,'sha256',sha256,'size',size_bytes,'rows',row_count) FROM datasets ORDER BY id;")
  for dataset in datasets:
   raw=c.raw('/datasets/'+dataset['id']+'/download');assert hashlib.sha256(raw).hexdigest()==dataset['sha256'] and len(raw)==dataset['size']
  phase_result=dict(ok=True,backendImage=backend,frontendImage=frontend,tableCounts=counts,datasets=datasets,allOriginalDatasetFingerprintsVerified=True,api=c.base,isolatedProject=project)
  if phase=='rollback-old':
   old_counts=counts;old_datasets=datasets
   if 'flyway_schema_history' in tables:phase_result['migrations']=project_object.rows("SELECT JSON_OBJECT('version',version,'success',success,'type',type) FROM flyway_schema_history ORDER BY installed_rank;")
  else:
   assert all(counts[k]==v for k,v in old_counts.items() if k!='flyway_schema_history');assert datasets==old_datasets
   migrations=project_object.rows("SELECT JSON_OBJECT('version',version,'success',success,'type',type) FROM flyway_schema_history ORDER BY installed_rank;");assert len(migrations)==6 and all(m['success']==1 for m in migrations);assert [m['version'] for m in migrations]==['1','2','3','4','5','6'];phase_result['migrations']=migrations
   subprocess.run(cmd+['stop','backend'],check=True,stdout=subprocess.DEVNULL,timeout=30);phase_result['audit']=st.audit(project_object);assert phase_result['audit']['ok']
  save(work/'verification.json',phase_result);result['phases'][phase]=phase_result
  subprocess.run(cmd+['stop'],check=True,stdout=subprocess.DEVNULL,timeout=90);print(phase+' verified; own cloned containers stopped',flush=True)
 assert ex.primary_fingerprints()==original['sourceFingerprints'];result.update(ok=True,actualMainUnchanged=True,scope='Actual stopped main cold backup restored into fresh volumes twice: old-image rollback and guarded candidate migration; main never started')
except Exception:
 result['failure']=traceback.format_exc()
 for project in json.loads((root/'projects.json').read_text()):
  for cid in output(['docker','ps','-aq','--filter','label=com.docker.compose.project='+project]).split():
   log=subprocess.check_output(['docker','logs','--tail','100',cid],stderr=subprocess.STDOUT);(root/(cid[:12]+'.log')).write_bytes(log)
 raise
finally:
 states=stop(root);result['containers']=states;result['allOwnedStopped']=all(n['stopped'] for n in states);save(root/'summary.json',result);(root/'guard.stop').touch();print(json.dumps(dict(root=str(root),ok=result['ok'],failure=result.get('failure'))),flush=True)
