#!/usr/bin/env python3
"""Control a verified localhost UI candidate. Never switches the main or fixed demo."""
from pathlib import Path
import argparse,hashlib,json,re,secrets,shutil,subprocess,sys,time,urllib.request
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'infrastructure'))
import storage as st
POINTER=ROOT/'.runtime/ui-candidate-current.json'
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def context():
 node=json.loads(POINTER.read_text());root=Path(node['root']).resolve();root.relative_to(ROOT/'.runtime')
 assert root.name.startswith('t11-1-ui-') and re.fullmatch('spark-dq-restore-[a-f0-9]{12}',node['project'])
 cfg=root/'deployment/compose.yaml';env=root/'deployment/.env'
 assert digest(cfg)==node['composeSha256'] and digest(env)==node['envSha256'],'Candidate controls changed; inspect before operating'
 receipt=json.loads((root/'deployment/receipt.json').read_text());assert receipt['project']==node['project']
 assert node['url']=='http://127.0.0.1:15176' and node['mainDeploymentTouched'] is False
 return root,node,st.Project(cfg,env)
def active(project):return int(project.mysql("SELECT (SELECT COUNT(*) FROM tasks WHERE status IN ('QUEUED','RUNNING','CANCELLING'))+(SELECT COUNT(*) FROM dataset_imports WHERE status IN ('QUEUED','RUNNING','CANCELLING'));"))
def verify(node,project):
 front=json.loads(subprocess.check_output(['docker','inspect',project.service('frontend')],text=True))[0]
 assert front['Image']==node['frontendImageId']
 backend=project.service('backend')
 for path,key in [('/app/backend.jar','backendJarSha256'),('/app/engine/spark-engine-0.1.0.jar','engineJarSha256')]:
  assert subprocess.check_output(['docker','exec',backend,'sha256sum',path],text=True).split()[0]==node[key]
def start(minutes):
 root,node,project=context();assert shutil.disk_usage(ROOT).free>5*1024**3
 subprocess.run(project.compose+['up','-d','--no-build','--pull','never'],check=True,timeout=120,stdout=subprocess.DEVNULL)
 deadline=time.monotonic()+90;local=urllib.request.build_opener(urllib.request.ProxyHandler({}))
 while True:
  try:
   with local.open(node['url']+'/api/health',timeout=3) as response:assert json.load(response)['status']=='UP'
   break
  except Exception:assert time.monotonic()<deadline;time.sleep(1)
 verify(node,project);lease=dict(token=secrets.token_hex(12),expiresAt=time.time()+minutes*60)
 (root/'ui-lease.json').write_text(json.dumps(lease))
 with (root/'ui-lease.log').open('a') as log:subprocess.Popen(['python3',str(Path(__file__).resolve()),'lease',lease['token']],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 print(json.dumps(dict(url=node['url'],expiresAt=lease['expiresAt'],mainDeploymentTouched=False),ensure_ascii=False))
def stop():
 root,node,project=context()
 if project.running('backend'):assert active(project)==0,'Candidate has active work; cancel or wait in the UI first'
 subprocess.run(project.compose+['stop'],check=True,timeout=90,stdout=subprocess.DEVNULL)
 (root/'ui-lease.json').write_text(json.dumps(dict(token='stopped',expiresAt=0)))
 print('候选已停止，所有文件、卷和网络保留；原主环境与固定演示未改动。')
def lease(token):
 root,node,project=context()
 while True:
  state=json.loads((root/'ui-lease.json').read_text())
  if state['token']!=token:return
  if time.time()>=state['expiresAt']:break
  time.sleep(5)
 # Never interrupt an import or Spark task simply because a review session expired.
 deadline=time.monotonic()+1800
 while project.running('backend') and active(project):
  if time.monotonic()>deadline:raise RuntimeError('Active work prevented expiry stop; data retained')
  if json.loads((root/'ui-lease.json').read_text())['token']!=token:return
  time.sleep(5)
 if json.loads((root/'ui-lease.json').read_text())['token']==token:stop()
def main():
 parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='action',required=True)
 p=sub.add_parser('start');p.add_argument('--minutes',type=int,default=120,choices=range(10,481),metavar='10..480')
 sub.add_parser('stop');sub.add_parser('status');p=sub.add_parser('lease');p.add_argument('token');args=parser.parse_args()
 if args.action=='start':start(args.minutes)
 elif args.action=='stop':stop()
 elif args.action=='lease':lease(args.token)
 else:
  root,node,project=context();print(json.dumps(dict(url=node['url'],project=node['project'],running={name:project.running(name) for name in ['frontend','backend','mysql','business']},mainDeploymentTouched=False),ensure_ascii=False))
if __name__=='__main__':main()
