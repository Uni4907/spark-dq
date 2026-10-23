"""Fresh local-only acceptance project. No primary mounts, credentials or deployment changes."""
import json,os,re,secrets,subprocess,time,uuid,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];WORK=ROOT/'.runtime/algorithm-tools';WORK.mkdir(parents=True,exist_ok=True)
def run(args,timeout=120,**kw):return subprocess.run(args,check=True,timeout=timeout,**kw)
def build(batch):
 assert re.fullmatch(r'[a-z0-9][a-z0-9_.-]{0,63}',batch)
 for role in ('backend','frontend'):
  with (WORK/(batch+'-build-'+role+'.log')).open('x') as log:
   run(['docker','build','-f','infrastructure/'+role+'.Dockerfile','-t','spark-dq-'+role+':'+batch,'.'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,timeout=600)
 run(['docker','run','--rm','--entrypoint','sh','spark-dq-frontend:'+batch,'-c','node --test tests/*.test.mjs'],timeout=90)
def create(batch,suffix=''):
 assert re.fullmatch(r'[a-z0-9][a-z0-9_.-]{0,63}',batch) and re.fullmatch(r'[a-z0-9_.-]*',suffix)
 root=ROOT/'.runtime'/(batch+suffix);root.mkdir(mode=0o700)
 name='sparkdq-alg-'+uuid.uuid4().hex[:12]
 template=(ROOT/'infrastructure/compose.yaml').read_text().replace('name: spark-dq','name: '+name)
 for role in ('backend','frontend'):
  template=template.replace('spark-dq-'+role+':0.1.0','spark-dq-'+role+':'+batch)
 template=template.replace('127.0.0.1:18080:8080','127.0.0.1::8080').replace('127.0.0.1:15173:5173','127.0.0.1::5173')
 # Pin resource bounds; Spark child in backend uses its existing 1GiB heap.
 template=template.replace('\n  backend:\n','\n  backend:\n    mem_limit: 3g\n    cpus: 2\n').replace('\n  mysql:\n','\n  mysql:\n    mem_limit: 768m\n').replace('\n  frontend:\n','\n  frontend:\n    mem_limit: 512m\n')
 template=template.replace('    build:\n      context: ..\n      dockerfile: infrastructure/backend.Dockerfile\n','').replace('    build:\n      context: ..\n      dockerfile: infrastructure/frontend.Dockerfile\n','')
 import sys
 sys.path.insert(0,str(ROOT/'infrastructure'))
 import cluster_lab
 plan=cluster_lab.network_plan()
 template+='\nnetworks:\n  default:\n    ipam:\n      config:\n        - subnet: '+plan['default']+'\n'
 (root/'network-plan.json').write_text(json.dumps(plan,indent=2))
 (root/'compose.yaml').write_text(template)
 fd=os.open(root/'.env',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 with os.fdopen(fd,'w') as f:f.write('MYSQL_PASSWORD='+secrets.token_hex(24)+'\nMYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\n')
 command=['docker','compose','--env-file',str(root/'.env'),'-f',str(root/'compose.yaml')]
 before=subprocess.check_output(['docker','ps','-a','--format','{{.Names}} {{.Image}} {{.Status}}'],timeout=30).decode()
 (root/'containers-before.txt').write_text(before)
 with (root/'startup.log').open('x') as log:run(command+['up','-d','--no-build','--pull','never'],stdout=log,stderr=subprocess.STDOUT,timeout=180)
 api='http://'+subprocess.check_output(command+['port','backend','8080'],timeout=20).decode().strip()+'/api'
 web='http://'+subprocess.check_output(command+['port','frontend','5173'],timeout=20).decode().strip()
 local=urllib.request.build_opener(urllib.request.ProxyHandler({}));deadline=time.monotonic()+120
 while time.monotonic()<deadline:
  try:
   with local.open(api+'/health',timeout=3) as res:assert json.load(res)['status']=='UP'
   break
  except Exception:time.sleep(1)
 else:raise TimeoutError('Isolated backend health')
 images={role:subprocess.check_output(command+['images','-q',role],timeout=30).decode().strip() for role in ('backend','frontend','mysql')}
 node={'batch':batch,'project':name,'api':api,'web':web,'images':images,'createdAt':time.strftime('%Y-%m-%dT%H:%M:%S%z'),'mainDeploymentTouched':False}
 (root/'lab.json').write_text(json.dumps(node,indent=2));print(json.dumps(node),flush=True);return root,node
if __name__=='__main__':
 import sys
 build(sys.argv[1]);create(sys.argv[1])
