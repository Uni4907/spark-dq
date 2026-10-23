#!/usr/bin/env python3
"""Expiring isolated product fixture; full-size inputs never cross HTTP upload."""
from pathlib import Path
import os,json,secrets,shutil,subprocess,sys,time,hashlib,urllib.request
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'infrastructure'))
import storage as st,cluster_compare,scale_oracle as oracle
def save(p,v):p.write_text(json.dumps(v,ensure_ascii=False,indent=2))
def output(args):return subprocess.check_output(args,text=True,timeout=60).strip()
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as stream:
  for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
 return h.hexdigest()
def context(root):
 root=Path(root).resolve();assert root.parent==ROOT/'.runtime' and root.name.startswith('t7-product-');meta=json.loads((root/'lab.json').read_text());return root,meta,['docker','compose','--env-file',str(root/'.env'),'-f',str(root/'compose.yaml')]
def stop(root):
 root,m,cmd=context(root);states=[]
 for cid in output(['docker','ps','-aq','--filter','label=com.docker.compose.project='+m['project']]).split():
  node=json.loads(output(['docker','inspect','--format','{{json .}}',cid]));assert node['Config']['Labels']['com.docker.compose.project']==m['project']
  if node['State']['Running']:subprocess.run(['docker','stop','--timeout','5',cid],check=True,stdout=subprocess.DEVNULL,timeout=25)
  states.append(dict(id=cid,stopped=output(['docker','inspect','--format','{{.State.Running}}',cid])=='false'))
 save(root/'cleanup.json',dict(allStopped=all(n['stopped'] for n in states),containers=states,volumesAndNetworksRetained=True));(root/'stop.request').touch()
def prepare():
 root=ROOT/'.runtime'/('t7-product-'+time.strftime('%Y%m%d-%H%M%S')+'-'+secrets.token_hex(3));root.mkdir(mode=0o700)
 assert min(shutil.disk_usage(root).free,shutil.disk_usage('/mnt/c').free)>=12*1024**3
 available=int(next(l.split()[1] for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith('MemAvailable:')))*1024;assert available>=6*1024**3
 original=oracle.row;source=oracle.prepare(root/'source-oracle',1000000)
 try:
  oracle.row=lambda i:dict(original(2*i),padding='REPEAT');governed=oracle.prepare(root/'governed-oracle',500000)
 finally:oracle.row=original
 for expected,n,folder,duplicate in [(source,1000000,root/'source-oracle',2),(governed,500000,root/'governed-oracle',1)]:
  raw=folder/'duplicate.expected.unsorted'
  with raw.open('w') as stream:
   if duplicate==2:
    for i in range(n):stream.write(oracle.encode(dict(original(i),__dq_group_size=2)))
  expected['metrics']['duplicate']=dict(duplicateGroups=n//2 if duplicate==2 else 0,duplicateRows=n if duplicate==2 else 0,excessRows=n//2 if duplicate==2 else 0);expected['anomalies']['duplicate']=oracle.sorted_digest(raw,folder/'duplicate.expected.sorted');save(folder/'expected.json',expected)
 inputs=root/'inputs';inputs.mkdir(mode=0o755);shutil.copyfile(root/'source-oracle/input.csv',inputs/'million.csv');(inputs/'million.csv').chmod(0o444);inputs.chmod(0o555);assert 20*1024**2<(inputs/'million.csv').stat().st_size<=64*1024**2
 save(root/'inputs.json',dict(rows=1000000,bytes=(inputs/'million.csv').stat().st_size,sha256=sha(inputs/'million.csv'),oraclePolicy=source['memoryPolicy'],availableMemoryBytes=available));print(json.dumps(dict(root=str(root),inputs=json.loads((root/'inputs.json').read_text()))),flush=True)
def start(root):
 root=Path(root).resolve();assert root.parent==ROOT/'.runtime' and root.name.startswith('t7-product-');assert not output(['docker','ps','-q'])
 candidate=json.loads(sorted((ROOT/'.runtime').glob('t7-build-*/candidate.json'))[-1].read_text());fingerprint=json.loads((root/'inputs.json').read_text())['sha256'];project='sparkdq-t7-'+secrets.token_hex(6)
 config=root/'controlled-files.json';save(config,dict(files=[dict(id='million',name='million.csv',path='/run/controlled-inputs/million.csv',sha256=fingerprint),dict(id='bad_hash',name='bad-hash.csv',path='/run/controlled-inputs/million.csv',sha256='0'*64)]));os.chown(config,1000,1000);config.chmod(0o400)
 (root/'.env').write_text('MYSQL_PASSWORD='+secrets.token_hex(24)+'\nMYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\n');(root/'.env').chmod(0o600)
 template=(ROOT/'infrastructure/compose.yaml').read_text().replace('name: spark-dq','name: '+project).replace('spark-dq-backend:0.1.0',candidate['backendImage']).replace('spark-dq-frontend:0.1.0',candidate['frontendImage'])
 for role in ['backend','frontend']:template=template.replace('    build:\n      context: ..\n      dockerfile: infrastructure/'+role+'.Dockerfile\n','')
 template=template.replace('127.0.0.1:18080:8080','127.0.0.1::8080').replace('127.0.0.1:15173:5173','127.0.0.1::5173').replace('\n  backend:\n','\n  backend:\n    cpus: 2\n    mem_limit: 3g\n').replace('\n  mysql:\n','\n  mysql:\n    cpus: 1\n    mem_limit: 768m\n').replace('\n  frontend:\n','\n  frontend:\n    mem_limit: 256m\n')
 template=template.replace('      SPARK_LOCAL_IP: 127.0.0.1','      SPARK_LOCAL_IP: 127.0.0.1\n      SPARK_DQ_CONTROLLED_FILES_FILE: /run/controlled-files.json\n      SPARK_INPUT_PARTITIONS: 4\n      SPARK_SHUFFLE_PARTITIONS: 2\n      SERVER_TOMCAT_ACCESSLOG_ENABLED: "true"\n      SERVER_TOMCAT_ACCESSLOG_DIRECTORY: /data/t7-http').replace('      - files:/data','      - files:/data\n      - '+str(config)+':/run/controlled-files.json:ro\n      - '+str(root/'inputs')+':/run/controlled-inputs:ro')
 ids=output(['docker','network','ls','-q']).split();nets=json.loads(output(['docker','network','inspect',*ids])) if ids else [];used=[c['Subnet'] for n in nets for c in (n.get('IPAM',{}).get('Config') or []) if c.get('Subnet')]+[r['dst'] for r in json.loads(output(['ip','-j','route','show','table','all'])) if r.get('dst')];subnet=cluster_compare.choose_subnet(used);template+='\nnetworks:\n  default:\n    ipam:\n      config:\n        - subnet: '+subnet+'\n';(root/'compose.yaml').write_text(template)
 save(root/'lab.json',dict(project=project,candidate=candidate,expiresAt=time.time()+3600,leaseSeconds=3600,subnet=subnet,mainDeploymentTouched=False,workerHeapMiB=1024,backendMiB=3072,inputPartitions=4,shufflePartitions=2,taskTimeoutSeconds=300,scope='Formal product local API/browser; not a performance comparison'))
 with (root/'lease.log').open('x') as log:child=subprocess.Popen(['python3',str(Path(__file__).resolve()),'lease',str(root)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 save(root/'lease-process.json',dict(pid=child.pid));r,m,cmd=context(root)
 try:
  with (root/'startup.log').open('x') as log:subprocess.run(cmd+['up','-d','--no-build','--pull','never'],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
  access=dict(frontendUrl='http://'+output(cmd+['port','frontend','5173']),apiUrl='http://'+output(cmd+['port','backend','8080'])+'/api',backendContainer=output(cmd+['ps','-q','backend']))
  deadline=time.monotonic()+100
  while True:
   try:
    with urllib.request.urlopen(access['apiUrl']+'/health',timeout=3) as response:assert json.load(response)['status']=='UP';break
   except Exception:assert time.monotonic()<deadline;time.sleep(1)
  assert output(['docker','exec',access['backendContainer'],'sha256sum','/app/engine/spark-engine-0.1.0.jar']).split()[0]==candidate['engineJarSha256'];save(root/'access.json',access);print(json.dumps(access),flush=True)
 except Exception:stop(root);raise
if __name__=='__main__':
 if sys.argv[1]=='prepare':prepare()
 elif sys.argv[1]=='start':start(sys.argv[2])
 elif sys.argv[1]=='stop':stop(sys.argv[2])
 elif sys.argv[1]=='lease':
  root,m,cmd=context(sys.argv[2])
  while time.time()<m['expiresAt'] and not (root/'stop.request').exists():time.sleep(5)
  stop(root)
