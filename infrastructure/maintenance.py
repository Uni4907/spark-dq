#!/usr/bin/env python3
"""One idle maintenance backup, with admission gate and resumable recovery.
No schedule, deletion, image recreation, volume restore or credential output.
"""
import argparse,json,os,signal,time,urllib.error,urllib.request,uuid
from pathlib import Path
import storage as st
KEY_PATH='/tmp/sparkdq-maintenance.key'

def persist(path,value):
    temporary=path.with_name('.receipt-'+uuid.uuid4().hex+'.tmp')
    created=False
    try:
        with temporary.open('x') as stream:
            created=True
            os.chmod(temporary,0o600);json.dump(value,stream,ensure_ascii=False,indent=2);stream.write('\n')
            stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:
        if created and temporary.exists():temporary.unlink()

class Control:
    def __init__(self,project):self.project=project
    def address(self):
        address=st.run(self.project.compose+['port','backend','8080']).decode().strip()
        if not __import__('re').fullmatch(r'127[.]0[.]0[.]1:[0-9]+',address):
            raise st.StorageError('Expected loopback backend binding')
        return 'http://'+address+'/api'
    def api(self,action='',owner=None):
        key=st.run(['docker','exec',self.project.service('backend'),'cat',KEY_PATH]).decode().strip()
        if not __import__('re').fullmatch('[0-9a-f]{64}',key):raise st.StorageError('Maintenance control key unavailable')
        headers={'X-SparkDQ-Maintenance-Key':key}
        body=None if owner is None else json.dumps({'owner':owner}).encode()
        if body is not None:headers['Content-Type']='application/json'
        request=urllib.request.Request(self.address()+'/internal/maintenance'+action,data=body,headers=headers)
        try:
            with st.LOCAL.open(request,timeout=20) as response:return json.load(response)
        except urllib.error.HTTPError as error:
            raise st.StorageError('Maintenance API refused request (HTTP '+str(error.code)+')') from None
    def healthy(self):
        with st.LOCAL.open(self.address()+'/health',timeout=4) as response:value=json.load(response)
        return value.get('status')=='UP'
    def wait(self):
        end=time.monotonic()+100
        while time.monotonic()<end:
            try:
                if self.healthy():return
            except (OSError,ValueError,st.StorageError):pass
            time.sleep(1)
        raise st.StorageError('Backend health timeout; maintenance receipt retained')

def estimate(project,parent,reserve_mib,host_capacity=None):
    raw=st.run(['docker','exec',project.service('backend'),'du','-sb','/data','/app']).decode().splitlines()
    application_and_files=sum(int(line.split()[0]) for line in raw)
    database=int(project.mysql("SELECT COALESCE(SUM(DATA_LENGTH+INDEX_LENGTH),0) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE();").strip())
    required=int(application_and_files*1.5)+max(database*4,64*1024**2)+reserve_mib*1024**2
    space=__import__('shutil').disk_usage(parent)
    result={'requiredFreeBytesEstimate':required,'linuxFreeBytes':space.free,'hostCapacityKnown':False}
    if space.free<required:raise st.StorageError('Insufficient Linux space for backup estimate')
    if host_capacity is not None:
        data=json.loads(host_capacity.read_text(encoding='utf-8-sig'))
        if not data.get('known') or type(data.get('freeBytes')) is not int:raise st.StorageError('Host capacity is unknown')
        result.update(hostCapacityKnown=True,hostFreeBytes=data['freeBytes'])
        if data['freeBytes']<required:raise st.StorageError('Insufficient host backing-volume space for estimate')
    return result

def recover(project,receipt,path):
    if project.service('backend')!=receipt['backendContainerId']:
        raise st.StorageError('Backend container identity changed; refusing automatic recovery')
    control=Control(project)
    if not project.running('backend'):
        st.run(project.compose+['start','backend'],timeout=90)
    control.wait()
    current=control.api()
    if current['maintenance'] and current['owner']!=receipt['owner']:
        raise st.StorageError('Different maintenance owner; protection retained')
    control.api('/leave',receipt['owner'])
    if control.api()['maintenance']:raise st.StorageError('Maintenance gate remains closed')
    receipt.update(serviceResumed=True,maintenanceReleased=True)
    persist(path,receipt)

def backup_once(project,output,reserve_mib=1024,host_capacity=None):
    output=output.absolute()
    if output.exists():raise st.StorageError('Refusing to reuse an existing output directory')
    if not output.parent.is_dir():raise st.StorageError('Output parent must exist')
    if not project.running('backend') or not project.running('mysql'):
        raise st.StorageError('Backend and MySQL must initially be running; prior stopped state preserved')
    control=Control(project)
    if not control.healthy():raise st.StorageError('Backend is not healthy')
    state=control.api()
    if state['maintenance']:raise st.StorageError('Another maintenance session already owns admission')
    capacity=estimate(project,output.parent,reserve_mib,host_capacity)
    output.mkdir(mode=0o700);path=output/'receipt.json'
    receipt={'formatVersion':'sparkdq-maintenance-v1','owner':str(uuid.uuid4()),
      'backendContainerId':project.service('backend'),'compose':project.compose,
      'output':str(output),'state':'prepared','capacity':capacity,
      'serviceResumed':False,'maintenanceReleased':False}
    persist(path,receipt);attempted=False;operation_error=None;recovery_error=None
    try:
        attempted=True
        control.api('/enter',receipt['owner'])
        receipt['state']='guarded';persist(path,receipt)
        st.run(project.compose+['stop','backend'],timeout=60)
        receipt['state']='offline';persist(path,receipt)
        result=st.backup(project,output/'backup')
        st.verify(output/'backup')
        receipt.update(state='backup-verified',backup=result);persist(path,receipt)
    except BaseException as error:
        operation_error=error;receipt.update(state='failed',failureType=type(error).__name__)
    finally:
        if attempted:
            try:recover(project,receipt,path)
            except BaseException as error:recovery_error=error;receipt['recoveryFailureType']=type(error).__name__
        receipt['needsResume']=attempted and not receipt['maintenanceReleased']
        if operation_error is None and recovery_error is None:receipt['state']='complete'
        persist(path,receipt)
    if recovery_error:raise st.StorageError('Service recovery incomplete; use resume with retained receipt') from None
    if operation_error:
        if isinstance(operation_error,(KeyboardInterrupt,SystemExit)):raise operation_error
        raise st.StorageError('Backup did not complete; service recovery succeeded; partial backup retained') from None
    return receipt

def resume(path):
    if path.is_symlink() or path.stat().st_size>2*1024*1024:raise st.StorageError('Unsafe receipt')
    receipt=json.loads(path.read_text())
    if receipt.get('formatVersion')!='sparkdq-maintenance-v1':raise st.StorageError('Unsupported receipt')
    if str(uuid.UUID(receipt['owner']))!=receipt['owner']:raise st.StorageError('Invalid owner')
    compose=receipt['compose']
    if not isinstance(compose,list) or len(compose)!=6 or compose[:3]!=['docker','compose','--env-file'] or compose[4]!='-f':
        raise st.StorageError('Invalid recorded compose arguments')
    project=st.Project(Path(compose[5]),Path(compose[3]))
    recover(project,receipt,path);receipt['needsResume']=False
    if receipt['state']!='complete':receipt.update(recoveredFrom=receipt['state'],state='recovered')
    persist(path,receipt)
    return receipt

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path);p.add_argument('--env',type=Path)
    sub=p.add_subparsers(dest='command',required=True)
    b=sub.add_parser('backup');b.add_argument('--output',type=Path,required=True)
    b.add_argument('--reserve-mib',type=int,default=1024);b.add_argument('--host-capacity',type=Path)
    s=sub.add_parser('resume');s.add_argument('receipt',type=Path)
    args=p.parse_args()
    if args.command=='backup' and not 256<=args.reserve_mib<=1048576:p.error('reserve-mib must be 256..1048576')
    def interrupted(signum,frame):raise InterruptedError('Maintenance interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    try:
        with st.lock():
            if args.command=='resume':result=resume(args.receipt.absolute())
            else:result=backup_once(st.Project(args.config,args.env),args.output,args.reserve_mib,args.host_capacity)
        print(json.dumps({'state':result['state'],'output':result['output'],
          'serviceResumed':result['serviceResumed'],'maintenanceReleased':result['maintenanceReleased'],
          'needsResume':result.get('needsResume',False)},ensure_ascii=False));return 0
    except (OSError,ValueError,KeyError,TypeError,st.StorageError) as error:
        print(json.dumps({'error':str(error),'type':type(error).__name__}),file=__import__('sys').stderr);return 1

if __name__=='__main__':__import__('sys').exit(main())
