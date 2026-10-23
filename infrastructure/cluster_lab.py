#!/usr/bin/env python3
"""Owned, isolated fixed-topology demo. No primary volumes or global startup configuration."""
import argparse,contextlib,datetime as dt,fcntl,json,os,re,secrets,shutil,signal,subprocess,sys,time,uuid
from pathlib import Path
import storage as st,runtime as rt
from cluster_compare import choose_subnet
LABEL='sparkdq.lab'
def save_state(root,node):
 temporary=root/('.state-'+uuid.uuid4().hex+'.tmp')
 try:
  st.save_json(temporary,node);os.replace(temporary,root/'state.json')
 finally:temporary.unlink(missing_ok=True)
def load(root):
 root=root.resolve();root.relative_to(st.ROOT/'.runtime');node=json.loads((root/'created.json').read_text())
 assert re.fullmatch('spark-dq-lab-[0-9a-f]{12}',node['project']) and node['configSha256']==st.digest(root/'compose.yaml')['sha256'] and node['envSha256']==st.digest(root/'.env')['sha256'],'Lab control files changed; no operation performed'
 values=dict(line.split('=',1) for line in (root/'.env').read_text().splitlines() if '=' in line)
 assert values.get('LAB_PROJECT')==node['project'],'Lab project identity mismatch'
 project=st.Project(root/'compose.yaml',root/'.env')
 keys=('LAB_PROJECT','LAB_BACKEND_IMAGE','LAB_FRONTEND_IMAGE','LAB_WORKER_IMAGE','MYSQL_PASSWORD','MYSQL_ROOT_PASSWORD')
 project.compose=['env',*(part for key in keys for part in ('-u',key)),*project.compose,'--project-name',node['project']]
 return root,node,project
def owned(root,node,project):
 for cid in st.run(['docker','ps','-aq','--filter','label=com.docker.compose.project='+node['project']]).decode().splitlines():
  assert st.run(['docker','inspect','--format','{{index .Config.Labels "'+LABEL+'"}}',cid]).decode().strip()==node['project'],'Foreign container; action refused'
 # Existing names must belong to this lab, even before Compose creates any containers.
 for name in st.run(['docker','network','ls','--format','{{.Name}}']).decode().splitlines():
  if name in (node['project']+'_default',node['project']+'_edge'):assert json.loads(st.run(['docker','network','inspect',name]))[0]['Labels'].get(LABEL)==node['project']
 for name in st.run(['docker','volume','ls','--format','{{.Name}}']).decode().splitlines():
  if name.startswith(node['project']+'_'):assert json.loads(st.run(['docker','volume','inspect',name]))[0]['Labels'].get(LABEL)==node['project']
def resolve_images(overrides=None):
 references={role:'spark-dq-'+role+':cluster-ui' for role in ('backend','frontend','worker')}
 overrides=overrides or {}
 if not isinstance(overrides,dict) or set(overrides)-set(references):raise ValueError('Unknown lab image role')
 references.update(overrides)
 pattern=r'(?:sha256:[0-9a-f]{64}|[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9_.-]+)?(?:@sha256:[0-9a-f]{64})?)'
 if any(not isinstance(value,str) or not re.fullmatch(pattern,value) for value in references.values()):raise ValueError('Invalid local image reference')
 images={role:st.run(['docker','image','inspect','--format','{{.Id}}',value]).decode().strip() for role,value in references.items()}
 if any(not re.fullmatch(r'sha256:[0-9a-f]{64}',value) for value in images.values()):raise ValueError('Invalid inspected image identity')
 return references,images

def allocate_subnets(used):
 default=choose_subnet(used);edge=choose_subnet([*used,default])
 return {'default':default,'edge':edge}
def network_plan():
 ids=st.run(['docker','network','ls','-q']).decode().split()
 nodes=json.loads(st.run(['docker','network','inspect',*ids])) if ids else []
 used=[cfg['Subnet'] for node in nodes for cfg in (node.get('IPAM',{}).get('Config') or []) if cfg.get('Subnet')]
 used.extend(route['dst'] for route in json.loads(st.run(['ip','-j','route','show','table','all'])) if route.get('dst'))
 return dict(allocate_subnets(used),observedSubnets=used)
def task_budget(seconds):
 if type(seconds) is not int or not 5<=seconds<=300:raise ValueError('Lab task budget must be 5..300 seconds')
 return seconds
def configure_budget(template,seconds):
 seconds=task_budget(seconds);key='      SPARK_DQ_EXECUTION: standalone'
 if template.count(key)!=1 or 'SPARK_DQ_TASK_TIMEOUT_SECONDS' in template:raise ValueError('Ambiguous lab budget configuration')
 return template.replace(key,key+'\n      SPARK_DQ_TASK_TIMEOUT_SECONDS: "'+str(seconds)+'"')
def create(root,capacity,image_overrides=None,task_timeout_seconds=300):
 task_budget(task_timeout_seconds)
 root=root.absolute();root.resolve().relative_to(st.ROOT/'.runtime');assert not root.exists() and root.parent.is_dir()
 host=json.loads(capacity.read_text(encoding='utf-8-sig'));assert host.get('known') and host['freeBytes']>=8*1024**3 and shutil.disk_usage(root.parent).free>=8*1024**3
 assert rt.active(st.Project())==0,'Primary active work; lab admission refused'
 references,images=resolve_images(image_overrides);networks=network_plan()
 project='spark-dq-lab-'+uuid.uuid4().hex[:12];root.mkdir(mode=0o700)
 template=configure_budget((st.ROOT/'infrastructure/cluster-lab.compose.yaml').read_text(),task_timeout_seconds)
 for role in ('default','edge'):
  original=role+': {'+('internal: true, ' if role=='default' else '')+'labels: *owned}'
  replacement=original[:-1]+', ipam: {config: [{subnet: '+networks[role]+'}]}}'
  assert template.count(original)==1;template=template.replace(original,replacement)
 (root/'compose.yaml').write_text(template)
 env='LAB_PROJECT='+project+'\n'+''.join('LAB_'+role.upper()+'_IMAGE='+value+'\n' for role,value in images.items())+'MYSQL_PASSWORD='+secrets.token_hex(24)+'\nMYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\n'
 with (root/'.env').open('x') as stream:os.chmod(root/'.env',0o600);stream.write(env)
 node={'version':1,'taskTimeoutSeconds':task_timeout_seconds,'project':project,'images':images,'imageReferences':references,'networkPlan':networks,'configSha256':st.digest(root/'compose.yaml')['sha256'],'envSha256':st.digest(root/'.env')['sha256'],'createdAt':rt.stamp(time.time())};st.save_json(root/'created.json',node)
 print(json.dumps({'created':True,'project':project,'output':str(root),'primaryVolumesUsed':False}),flush=True)
def address(project,service,port):
 value=st.run(project.compose+['port',service,str(port)]).decode().strip();assert re.fullmatch(r'127[.]0[.]0[.]1:[0-9]+',value);return 'http://'+value
def observed_addresses(state,project):
 if state.get('alive') and state.get('state')=='running':
  try:
   api=address(project,'backend',8080)+'/api';web=address(project,'frontend',5173)
   state.update(api=api,web=web,addressObservation='OBSERVED')
  except (st.StorageError,AssertionError):
   state.pop('api',None);state.pop('web',None);state['addressObservation']='UNAVAILABLE'
 return state
def status(root):
 root,node,project=load(root)
 try:state=json.loads((root/'state.json').read_text())
 except FileNotFoundError:return {'state':'created','alive':False,'project':node['project']}
 try:state['alive']=rt.ticks(int(state['pid']))==state['startTicks'] and all(part in (Path('/proc')/str(state['pid'])/'cmdline').read_bytes().split(b'\0') for part in (str(Path(__file__).resolve()).encode(),b'serve',str(root).encode()))
 except (OSError,ValueError,KeyError):state['alive']=False
 return observed_addresses(state,project)
def serve(root,seconds):
 root,node,project=load(root);assert 60<=seconds<=7200
 with (root/'serve.lock').open('a') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);owned(root,node,project)
  started=time.time();deadline=time.monotonic()+seconds;stopping=[False]
  def stop_requested(signum,frame):stopping[0]=True
  signal.signal(signal.SIGTERM,stop_requested);signal.signal(signal.SIGINT,stop_requested)
  state={'state':'starting','project':node['project'],'pid':os.getpid(),'startTicks':rt.ticks(os.getpid()),'startedAt':rt.stamp(started),'expiresAt':rt.stamp(started+seconds)};save_state(root,state)
  try:
   with (root/('startup-'+uuid.uuid4().hex+'.log')).open('xb') as output:
    started_process=subprocess.run(project.compose+['up','-d','--no-build','--pull','never'],stdout=output,stderr=subprocess.STDOUT,timeout=150,check=False)
   if started_process.returncode:raise st.StorageError('Lab startup failed; inspect private startup log')
   api=address(project,'backend',8080)+'/api';web=address(project,'frontend',5173);end=time.monotonic()+100
   while time.monotonic()<end and not stopping[0]:
    try:
     with st.LOCAL.open(api+'/health',timeout=3) as response:ready=json.load(response)['status']=='UP'
     with st.LOCAL.open(web,timeout=3) as response:ready=ready and response.status==200
     if ready:break
    except (OSError,ValueError):pass
    time.sleep(.5)
   else:raise st.StorageError('Lab health deadline exceeded')
   state.update(state='running',api=api,web=web);save_state(root,state);print(json.dumps({key:state[key] for key in ('state','project','api','web','expiresAt')}),flush=True)
   while not stopping[0] and not rt.reached(deadline,started+seconds):time.sleep(.5)
   drain=time.monotonic()+350
   while time.monotonic()<drain:
    try:
     if rt.active(project)==0:break
    except st.StorageError:break
    time.sleep(1)
  except BaseException as error:
   state.update(state='failed',reason=type(error).__name__);raise
  finally:
   owned(root,node,project);st.run(project.compose+['stop'],timeout=120)
   state.update(state='stopped' if state['state']!='failed' else 'failed',finishedAt=rt.stamp(time.time()));save_state(root,state)
def stop(root):
 state=status(root)
 if state.get('alive'):
  _,node,project=load(root);owned(root,node,project)
  if rt.active(project):raise st.StorageError('Active lab tasks; stop refused without cancelling user work')
  os.kill(state['pid'],signal.SIGTERM);end=time.monotonic()+20
  while time.monotonic()<end:
   if not status(root).get('alive'):break
   time.sleep(.25)
 return status(root)
def outage(root,seconds):
 root,node,project=load(root);assert 1<=seconds<=60
 with (root/'outage.lock').open('a') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);owned(root,node,project);assert rt.active(project)==0,'Active tasks; outage refused'
  cid=project.service('master');assert st.run(['docker','inspect','--format','{{.State.Paused}}',cid]).strip()==b'false','Already paused; no change performed'
  identity=uuid.uuid4().hex;requested=[False]
  def cancelled(signum,frame):requested[0]=True
  signal.signal(signal.SIGTERM,cancelled);signal.signal(signal.SIGINT,cancelled)
  try:
   st.run(['docker','pause',cid],timeout=15);st.save_json(root/('outage-'+identity+'-started.json'),{'project':node['project'],'master':cid,'seconds':seconds});print('Master paused for bounded browser fault acceptance',flush=True)
   deadline=time.monotonic()+seconds
   while time.monotonic()<deadline and not requested[0]:time.sleep(.1)
  finally:
   owned(root,node,project);st.run(['docker','unpause',cid],timeout=15);st.save_json(root/('outage-'+identity+'-resumed.json'),{'project':node['project'],'master':cid,'resumed':True});print('Owned Master resumed',flush=True)
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=('create','serve','status','stop','outage'));parser.add_argument('--output',type=Path,required=True);parser.add_argument('--host-capacity',type=Path);parser.add_argument('--seconds',type=int,default=3600);
 parser.add_argument('--task-timeout-seconds',type=int)
 for role in ('backend','frontend','worker'):parser.add_argument('--'+role+'-image')
 args=parser.parse_args();overrides={role:getattr(args,role+'_image') for role in ('backend','frontend','worker') if getattr(args,role+'_image') is not None}
 if overrides and args.action!='create':parser.error('Image selectors are only supported by create')
 if args.task_timeout_seconds is not None and args.action!='create':parser.error('Task budget is only supported by create')
 if args.action=='create':assert args.host_capacity;create(args.output,args.host_capacity,overrides,args.task_timeout_seconds if args.task_timeout_seconds is not None else 300)
 elif args.action=='serve':serve(args.output,args.seconds)
 elif args.action=='outage':outage(args.output,args.seconds)
 else:print(json.dumps(stop(args.output) if args.action=='stop' else status(args.output)),flush=True)
if __name__=='__main__':main()
