#!/usr/bin/env python3
"""Private, expiring T6 browser fixture. Stops only its own project; retains all data."""
from pathlib import Path
import contextlib,json,os,secrets,shutil,subprocess,sys,time,urllib.request,uuid
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'infrastructure'))
import cluster_compare,storage
def save(p,v):p.write_text(json.dumps(v,ensure_ascii=False,indent=2))
def run(args,**kw):return subprocess.run(args,check=True,timeout=kw.pop('timeout',120),**kw)
def out(args):return subprocess.check_output(args,text=True,timeout=45).strip()
def context(root):
 root=Path(root).resolve();assert root.parent==ROOT/'.runtime' and root.name.startswith('t6-ui-')
 lab=json.loads((root/'lab.json').read_text());return root,lab,['docker','compose','--env-file',str(root/'.env'),'-f',str(root/'compose.yaml')]
def stop(root):
 root,lab,cmd=context(root);ids=out(['docker','ps','-aq','--filter','label=com.docker.compose.project='+lab['project']]).split();states=[]
 for cid in ids:
  node=json.loads(out(['docker','inspect','--format','{{json .}}',cid]));assert node['Config']['Labels']['com.docker.compose.project']==lab['project']
  if node['State']['Running']:run(['docker','stop','--time','5',cid],stdout=subprocess.DEVNULL,timeout=25)
  states.append(dict(containerId=cid,stopped=out(['docker','inspect','--format','{{.State.Running}}',cid])=='false'))
 save(root/'cleanup.json',dict(allStopped=all(s['stopped'] for s in states),containers=states,networksAndVolumesRetained=True));(root/'stop.request').touch()
def lease(root):
 root,lab,cmd=context(root)
 while time.time()<lab['expiresAt'] and not (root/'stop.request').exists():time.sleep(5)
 try:stop(root)
 except Exception as e:save(root/'cleanup-error.json',dict(error=type(e).__name__))
def sql(root,statement):
 root,lab,cmd=context(root);cid=out(cmd+['ps','-q','business'])
 return subprocess.check_output(['docker','exec','-i',cid,'sh','-c','export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; exec mysql -u root --binary-mode --default-character-set=utf8mb4 --batch --raw --skip-column-names'],input=statement.encode(),timeout=50).decode()
def start():
 assert not out(['docker','ps','-q']),'Another workload is running'
 root=ROOT/'.runtime'/('t6-ui-'+time.strftime('%Y%m%d-%H%M%S')+'-'+secrets.token_hex(3));root.mkdir(mode=0o700)
 available=int(next(s.split()[1] for s in Path('/proc/meminfo').read_text().splitlines() if s.startswith('MemAvailable:')))*1024
 assert min(shutil.disk_usage(root).free,shutil.disk_usage('/mnt/c').free)>=8*1024**3 and available>=4*1024**3
 fixes=sorted((ROOT/'.runtime').glob('t6-frontend-final-*/candidate.json')) or sorted((ROOT/'.runtime').glob('t6-governance-fix-*/candidate.json'))
 candidate=json.loads((fixes[-1] if fixes else sorted((ROOT/'.runtime').glob('t1-build-*/candidate.json'))[-1]).read_text());front=candidate if fixes else json.loads(sorted((ROOT/'.runtime').glob('t6-build-*/candidate.json'))[-1].read_text());project='sparkdq-t6-'+secrets.token_hex(6)
 reader=secrets.token_hex(24);wrong=secrets.token_hex(24)
 for name,password in [('reader',reader),('wrong',wrong)]:
  f=root/(name+'.password');f.write_text(password);os.chown(f,1000,1000);f.chmod(0o400)
 (root/'.env').write_text('MYSQL_PASSWORD='+secrets.token_hex(24)+'\nMYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\nBUSINESS_ROOT_PASSWORD='+secrets.token_hex(24)+'\n');(root/'.env').chmod(0o600)
 fields=['id','amount','code','category','note'];sources=[]
 for name,limits in [('demo',{}),('row_limit',{'maxRows':2}),('wrong',{}),('blocked',{'timeoutSeconds':60})]:sources.append(dict(id=name,host='business',port=3306,user='snapshot_reader',passwordFile='/run/'+('wrong' if name=='wrong' else 'reader')+'.password',tlsMode='DISABLED',schema='business_demo',table='orders',columns=[dict(source=s,target=s) for s in fields],**limits))
 config=root/'sources.json';save(config,dict(sources=sources));os.chown(config,1000,1000);config.chmod(0o400)
 template=(ROOT/'infrastructure/compose.yaml').read_text().replace('name: spark-dq','name: '+project).replace('spark-dq-backend:0.1.0',candidate['backendImage']).replace('spark-dq-frontend:0.1.0',front['frontendImage'])
 for role in ('backend','frontend'):template=template.replace('    build:\n      context: ..\n      dockerfile: infrastructure/'+role+'.Dockerfile\n','')
 template=template.replace('127.0.0.1:18080:8080','127.0.0.1::8080').replace('127.0.0.1:15173:5173','127.0.0.1::5173').replace('\n  backend:\n','\n  backend:\n    mem_limit: 3g\n    cpus: 2\n').replace('\n  mysql:\n','\n  mysql:\n    mem_limit: 768m\n').replace('\n  frontend:\n','\n  frontend:\n    mem_limit: 256m\n')
 template=template.replace('      SPARK_LOCAL_IP: 127.0.0.1','      SERVER_TOMCAT_ACCESSLOG_ENABLED: "true"\n      SERVER_TOMCAT_ACCESSLOG_DIRECTORY: /data/t6-http\n      SPARK_LOCAL_IP: 127.0.0.1\n      SPARK_DQ_MYSQL_SOURCES_FILE: /run/mysql-sources.json').replace('      - files:/data','      - files:/data\n      - '+str(config)+':/run/mysql-sources.json:ro\n      - '+str(root/'reader.password')+':/run/reader.password:ro\n      - '+str(root/'wrong.password')+':/run/wrong.password:ro')
 business='\n'.join(['  business:','    image: '+storage.MYSQL_IMAGE,'    mem_limit: 768m','    cpus: 1','    environment:','      MYSQL_ROOT_PASSWORD: ${BUSINESS_ROOT_PASSWORD}','    volumes:','      - business-data:/var/lib/mysql',''])
 template=template.replace('\n  backend:\n','\n'+business+'  backend:\n').replace('\n  files:\n','\n  files:\n  business-data:\n')
 ids=out(['docker','network','ls','-q']).split();nets=json.loads(out(['docker','network','inspect',*ids])) if ids else [];used=[c['Subnet'] for n in nets for c in (n.get('IPAM',{}).get('Config') or []) if c.get('Subnet')];used += [r['dst'] for r in json.loads(out(['ip','-j','route','show','table','all'])) if r.get('dst')];subnet=cluster_compare.choose_subnet(used)
 template+='\nnetworks:\n  default:\n    ipam:\n      config:\n        - subnet: '+subnet+'\n';(root/'compose.yaml').write_text(template)
 save(root/'lab.json',dict(project=project,candidate=candidate,frontend=front,expiresAt=time.time()+2700,leaseSeconds=2700,subnet=subnet,availableMemoryBytes=available,mainDeploymentTouched=False,businessPublicPorts=[]));root,lab,cmd=context(root)
 with (root/'lease.log').open('x') as log:child=subprocess.Popen(['python3',str(Path(__file__).resolve()),'lease',str(root)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 save(root/'lease-process.json',dict(pid=child.pid,expiresAt=lab['expiresAt']))
 try:
  with (root/'startup.log').open('x') as log:run(cmd+['up','-d','--no-build','--pull','never'],stdout=log,stderr=subprocess.STDOUT,timeout=180)
  deadline=time.monotonic()+100
  while True:
   try:sql(root,'SELECT 1;');break
   except subprocess.CalledProcessError:
    assert time.monotonic()<deadline;time.sleep(1)
  sql(root,"CREATE DATABASE business_demo CHARACTER SET utf8mb4; CREATE TABLE business_demo.orders(id BIGINT,amount DECIMAL(20,2),code VARCHAR(64),category VARCHAR(64),note TEXT) ENGINE=InnoDB; CREATE USER 'snapshot_reader'@'%' IDENTIFIED BY '"+reader+"'; GRANT SELECT ON business_demo.orders TO 'snapshot_reader'@'%';")
  rows=[dict(id=str(i+1),amount=str(10+i)+'.00',code='A' if i<3 else 'B'+str(i//3),category='零售' if i!=2 else '批发',note=None if i%4==0 else ' ' if i==1 else '备注\r\n保留' if i==3 else '正常') for i in range(12)];rows[-1]['amount']='1200.00';rows.extend([dict(rows[2]),dict(rows[4])]);save(root/'fixture-rows.json',rows)
  def quote(v):return 'NULL' if v is None else "'"+v.replace('\\','\\\\').replace("'","''")+"'"
  sql(root,'INSERT INTO business_demo.orders VALUES '+','.join('('+','.join(quote(r[f]) for f in fields)+')' for r in rows)+';')
  backend=out(cmd+['ps','-q','backend']);url='http://'+out(cmd+['port','frontend','5173']);api='http://'+out(cmd+['port','backend','8080'])+'/api';ready=time.monotonic()+100
  while True:
   try:
    with urllib.request.urlopen(api+'/health',timeout=3) as response:assert json.load(response)['status']=='UP';break
   except Exception:assert time.monotonic()<ready;time.sleep(1)
  actual=out(['docker','exec',backend,'sha256sum','/app/engine/spark-engine-0.1.0.jar']).split()[0];assert actual==candidate['engineJarSha256'];save(root/'access.json',dict(frontendUrl=url,apiUrl=api,backendContainer=backend,engineJarSha256=actual));print(json.dumps(dict(root=str(root),frontendUrl=url,apiUrl=api,expiresAt=lab['expiresAt'])),flush=True)
 except Exception:stop(root);raise
if __name__=='__main__':
 if sys.argv[1]=='start':start()
 elif sys.argv[1]=='lease':lease(sys.argv[2])
 elif sys.argv[1]=='stop':stop(sys.argv[2])
