#!/usr/bin/env python3
"""Fixed localhost demonstration on separate restored volumes. Never switches main."""
from pathlib import Path
import argparse,json,subprocess,time,socket,shutil,sys,hashlib,re
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'infrastructure'));import storage as st
POINTER=ROOT/'.runtime/demo-current.json'
def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2))
def context():
 node=json.loads(POINTER.read_text());root=Path(node['root']).resolve();root.relative_to(ROOT/'.runtime');assert root.name.startswith('demo-release-');receipt=json.loads((root/'deployment/receipt.json').read_text());assert re.fullmatch('spark-dq-restore-[a-f0-9]{12}',receipt['project']) and receipt['project']==node['project'];return root,node,st.Project(root/'deployment/compose.yaml',root/'deployment/.env')
def start():
 root,node,project=context();assert shutil.disk_usage(ROOT).free>8*1024**3 and shutil.disk_usage('/mnt/c').free>8*1024**3
 subprocess.run(project.compose+['up','-d','--no-build','--pull','never'],check=True,timeout=120,stdout=subprocess.DEVNULL)
 import urllib.request
 local=urllib.request.build_opener(urllib.request.ProxyHandler({}));deadline=time.monotonic()+80
 while True:
  try:
   with local.open(node['api']+'/health',timeout=3) as response:assert json.load(response)['status']=='UP';break
  except Exception:assert time.monotonic()<deadline;time.sleep(1)
 cid=project.service('backend')
 for path,key in [('/app/backend.jar','backendJarSha256'),('/app/engine/spark-engine-0.1.0.jar','engineJarSha256')]:assert subprocess.check_output(['docker','exec',cid,'sha256sum',path],text=True,timeout=20).split()[0]==node[key]
 print(json.dumps(dict(url=node['url'],api=node['api'],project=node['project'],mainDeploymentTouched=False),ensure_ascii=False),flush=True);return root,node,project
def prepare(backup,frontend):
 assert not POINTER.exists(),'An existing demo is preserved; use start/status/stop'
 assert re.fullmatch('sha256:[a-f0-9]{64}',frontend);backup=backup.resolve();backup.relative_to(ROOT/'.runtime');st.verify(backup)
 for port in [15174,18081]:
  with socket.socket() as probe:probe.bind(('127.0.0.1',port))
 assert int(next(l.split()[1] for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith('MemAvailable:')))*1024>4*1024**3
 root=ROOT/'.runtime'/('demo-release-'+time.strftime('%Y%m%d-%H%M%S'));root.mkdir(mode=0o700);receipt=st.restore(backup,root/'deployment');project=st.Project(root/'deployment/compose.yaml',root/'deployment/.env')
 try:
  subprocess.run(project.compose+['stop'],check=True,timeout=60,stdout=subprocess.DEVNULL)
  config=root/'deployment/compose.yaml';text=config.read_text().replace('"127.0.0.1::8080"','"127.0.0.1:18081:8080"').replace('\n  backend:\n','\n  backend:\n    cpus: 2\n    mem_limit: 3g\n').replace('\n  mysql:\n','\n  mysql:\n    cpus: 1\n    mem_limit: 768m\n')
  text=text.replace('\nvolumes:\n','\n  frontend:\n    image: '+frontend+'\n    mem_limit: 256m\n    cpus: 0.5\n    ports: ["127.0.0.1:15174:5173"]\n    depends_on: [backend]\nvolumes:\n',1);config.write_text(text);ledger=json.loads((ROOT/'docs/release-candidate.json').read_text());node=dict(version=1,root=str(root),project=receipt['project'],url='http://127.0.0.1:15174',api='http://127.0.0.1:18081/api',frontendImageId=frontend,backendJarSha256=ledger['backendJarSha256'],engineJarSha256=ledger['engineJarSha256'],sourceCandidateImageId=ledger['candidate']['backendImageId'],baseImageId=st.verify(backup)['backendImageId'],application='Verified backup /app mounted read-only',sourceBackup=str(backup),sourceCommit=subprocess.check_output(['git','-c','safe.directory='+str(ROOT),'-C',str(ROOT),'rev-parse','HEAD'],text=True).strip(),mainDeploymentTouched=False);save(POINTER,node);start()
 except Exception:
  subprocess.run(project.compose+['stop'],timeout=60,stdout=subprocess.DEVNULL);raise
def stop():
 root,node,project=context()
 if project.running('backend'):
  n=int(project.mysql("SELECT (SELECT COUNT(*) FROM tasks WHERE status IN ('QUEUED','RUNNING','CANCELLING'))+(SELECT COUNT(*) FROM dataset_imports WHERE status IN ('QUEUED','RUNNING','CANCELLING'));"))
  if n:raise RuntimeError('演示仍有进行中任务，请先在页面取消并等待结束')
 subprocess.run(project.compose+['stop'],check=True,timeout=60,stdout=subprocess.DEVNULL);print('演示已停止，所有数据与卷保留；主环境未修改',flush=True)
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='action',required=True);p=sub.add_parser('prepare');p.add_argument('--backup',type=Path,required=True);p.add_argument('--frontend',required=True)
 for name in ['start','stop','status']:sub.add_parser(name)
 args=parser.parse_args()
 if args.action=='prepare':prepare(args.backup,args.frontend)
 elif args.action=='start':start()
 elif args.action=='stop':stop()
 else:
  root,node,project=context();print(json.dumps(dict(url=node['url'],api=node['api'],project=node['project'],backendRunning=project.running('backend'),mainDeploymentTouched=False),ensure_ascii=False))
