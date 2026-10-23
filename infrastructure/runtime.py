#!/usr/bin/env python3
"""Bounded foreground WSL session. No Windows/global startup configuration."""
import argparse,contextlib,datetime as dt,fcntl,json,os,signal,sys,time,uuid
from pathlib import Path
import storage as st

ROOT=Path(__file__).resolve().parent.parent
STATE=ROOT/'.runtime/service-session.json'
LOCK=ROOT/'.runtime/service-session.lock'
UTC=dt.timezone.utc

def stamp(seconds):
    return dt.datetime.fromtimestamp(seconds,UTC).isoformat()

def ticks(pid):
    return (Path('/proc')/str(pid)/'stat').read_text().rsplit(') ',1)[1].split()[19]

def belongs(state):
    try:
        pid=int(state['pid'])
        command=(Path('/proc')/str(pid)/'cmdline').read_bytes().split(b'\0')
        return str(Path(__file__).resolve()).encode() in command and b'serve' in command and ticks(pid)==state['startTicks']
    except (OSError,ValueError,KeyError):return False

def read_state():
    try:state=json.loads(STATE.read_text())
    except (OSError,ValueError):return {'state':'stopped','alive':False}
    state['alive']=belongs(state)
    if not state['alive'] and state.get('state') not in ('stopped','failed','expired'):
        state['state']='stale'
    return state

def write_state(state):
    STATE.parent.mkdir(exist_ok=True)
    temporary=STATE.with_name('.service-session-'+uuid.uuid4().hex+'.tmp')
    with temporary.open('x') as stream:
        os.chmod(temporary,0o600);json.dump(state,stream,ensure_ascii=False,indent=2);stream.write('\n')
    os.replace(temporary,STATE)

def active(project):
    count=int(project.mysql("SELECT COUNT(*) FROM tasks WHERE status IN ('QUEUED','RUNNING','CANCELLING');").strip())
    if b'dataset_imports' in project.mysql('SHOW TABLES;').splitlines():
        count+=int(project.mysql("SELECT COUNT(*) FROM dataset_imports WHERE status IN ('QUEUED','RUNNING','CANCELLING');").strip())
    return count

def service_health(project):
    if not project.running('backend'):return {'status':'STOPPED'}
    address=st.run(project.compose+['port','backend','8080']).decode().strip()
    if not address.startswith('127.0.0.1:'):raise st.StorageError('Unexpected non-loopback API address')
    with st.LOCAL.open('http://'+address+'/api/health',timeout=4) as response:
        value=json.load(response)
    return {key:value.get(key) for key in ('status','schemaVersion','database')}

def stop():
    state=read_state()
    if not state['alive']:return state
    if active(st.Project()):raise st.StorageError('Active tasks; session stop refused. No task was cancelled.')
    os.kill(int(state['pid']),signal.SIGTERM)
    end=time.monotonic()+12
    while time.monotonic()<end:
        current=read_state()
        if not current['alive'] or current.get('state')=='stop-refused':return current
        time.sleep(.25)
    raise st.StorageError('Session did not acknowledge stop; no containers were stopped')

def reached(monotonic_deadline,wall_deadline):
    # Wall time catches suspend/resume; monotonic time bounds backward clock changes.
    return time.monotonic()>=monotonic_deadline or time.time()>=wall_deadline

def serve(seconds):
    LOCK.parent.mkdir(exist_ok=True)
    with LOCK.open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return dict(read_state(),note='Existing session retained; duration was not renewed')
        started=time.time();deadline=time.monotonic()+seconds;drain_end=deadline+3000
        state={'state':'starting','sessionId':uuid.uuid4().hex,'pid':os.getpid(),
          'startTicks':ticks(os.getpid()),'startedAt':stamp(started),'expiresAt':stamp(started+seconds),
          'maximumDrainEndsAt':stamp(started+seconds+3000),'durationSeconds':seconds}
        write_state(state)
        stopping=[False]
        def request_stop(signum,frame):stopping[0]=True
        signal.signal(signal.SIGTERM,request_stop);signal.signal(signal.SIGINT,request_stop)
        project=st.Project()
        try:
            # Respect offline storage maintenance. Never build/pull or restart on failed health.
            with st.lock():
                try:backend_running=project.running('backend')
                except st.StorageError:backend_running=False
                if backend_running:
                    # Do not let compose recreate a live backend after a tag/config change.
                    try:frontend_running=project.running('frontend')
                    except st.StorageError:frontend_running=False
                    if not frontend_running:
                        st.run(project.compose+['up','-d','--no-deps','--no-build','--pull','never','frontend'],timeout=120)
                else:
                    st.run(project.compose+['up','-d','--no-build','--pull','never'],timeout=120)
            end=time.monotonic()+90
            while time.monotonic()<end:
                try:
                    health=service_health(project)
                    if health.get('status')=='UP':break
                except (OSError,ValueError,st.StorageError):pass
                time.sleep(1)
            else:raise st.StorageError('Startup health timeout; containers retained for diagnosis')
            state.update(state='running',health=health);write_state(state)
            while True:
                if stopping[0]:
                    try:count=active(project)
                    except st.StorageError:count=None
                    if count==0:
                        state.update(state='stopped',reason='Session stopped; Docker containers retained');break
                    stopping[0]=False
                    state.update(state='stop-refused',reason='Tasks active or database unavailable; session retained')
                if reached(deadline,started+seconds):
                    try:count=active(project)
                    except st.StorageError:count=None
                    if count==0 or reached(drain_end,started+seconds+3000):
                        state.update(state='expired',reason='Lease ended; containers retained',activeAtExit=count);break
                    state.update(state='draining',activeTasks=count)
                try:state['health']=service_health(project)
                except (OSError,ValueError,st.StorageError):state['health']={'status':'UNAVAILABLE'}
                state['checkedAt']=stamp(time.time());write_state(state)
                time.sleep(2)
        except Exception as error:
            state.update(state='failed',reason=type(error).__name__)
            raise
        finally:
            state['finishedAt']=stamp(time.time());write_state(state)
        return state

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='action',required=True)
    p=sub.add_parser('serve');p.add_argument('--seconds',type=int,default=43200)
    sub.add_parser('status');sub.add_parser('stop')
    args=parser.parse_args()
    if args.action=='serve' and not 1<=args.seconds<=43200:parser.error('seconds must be 1..43200')
    try:
        result=serve(args.seconds) if args.action=='serve' else stop() if args.action=='stop' else read_state()
        print(json.dumps(result,ensure_ascii=False));return 0
    except (OSError,ValueError,st.StorageError) as error:
        print(json.dumps({'error':str(error),'type':type(error).__name__}),file=sys.stderr);return 1

if __name__=='__main__':sys.exit(main())
