#!/usr/bin/env python3
"""Real session lifecycle + Spark checks; leaves all synthetic data and volumes."""
import json,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT/'infrastructure'))
import runtime as rt
import storage as st
import e2e

def launch(seconds):
    return subprocess.Popen(['python3',str(ROOT/'infrastructure/runtime.py'),'serve','--seconds',str(seconds)],
      stdout=subprocess.PIPE,stderr=subprocess.PIPE)

def await_state(name,timeout=30):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        state=rt.read_state()
        if state.get('state')==name and state['alive']:return state
        time.sleep(.1)
    raise AssertionError('Expected session state '+name+': '+str(rt.read_state()))

def main():
    assert not rt.read_state()['alive'],'Stop existing idle test session first; do not replace a live session'
    p=st.Project();assert rt.active(p)==0
    backend=p.service('backend')
    dataset=json.loads((ROOT/'.runtime/e2e.json').read_text())['dataset']
    code,old=e2e.http('/tasks/'+json.loads((ROOT/'.runtime/e2e.json').read_text())['task']['id'])
    assert code==200
    processes=[];report={'checks':[]}
    try:
        process=launch(120);processes.append(process)
        state=await_state('running')
        duplicate=subprocess.run(['python3',str(ROOT/'infrastructure/runtime.py'),'serve','--seconds','1'],
          stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=20)
        assert duplicate.returncode==0
        duplicate_state=json.loads(duplicate.stdout)
        assert duplicate_state['sessionId']==state['sessionId'] and duplicate_state['durationSeconds']==120
        assert p.service('backend')==backend,'Live backend was recreated'
        report['checks'].append('duplicate launch retains single session and live backend container')
        task=e2e.create(dataset['id'],['name','email'],['id'])
        end=time.monotonic()+10
        while time.monotonic()<end:
            code,current=e2e.http('/tasks/'+task['id'])
            if current['status']=='RUNNING':break
            time.sleep(.1)
        assert current['status']=='RUNNING',current
        try:rt.stop()
        except st.StorageError as error:assert 'Active tasks' in str(error)
        else:raise AssertionError('Busy session stop should be refused')
        assert rt.read_state()['sessionId']==state['sessionId'] and process.poll() is None
        done=e2e.wait(task['id']);assert done['status']=='SUCCEEDED',done
        assert rt.stop()['state']=='stopped';assert process.wait(timeout=15)==0
        assert e2e.http('/health')[1]['status']=='UP'
        report['checks'].append('active Spark refuses stop; job succeeds; idle stop retains healthy containers')
        # Expiry while Spark is running drains the job before releasing the session.
        rows=['id,name']+[str(n//2)+(',' if n%4==0 else ',value') for n in range(100000)]
        code,big=e2e.upload(('\n'.join(rows)+'\n').encode(),'session-drain-synthetic.csv')
        assert code==200 and big['row_count']==100000,big
        task=e2e.create(big['id'],['name'],['id'])
        process=launch(1);processes.append(process)
        draining=await_state('draining',timeout=35)
        assert draining['activeTasks']>0,draining
        done=e2e.wait(task['id']);assert done['status']=='SUCCEEDED',done
        assert process.wait(timeout=30)==0
        expired=rt.read_state();assert expired['state']=='expired' and expired['activeAtExit']==0 and not expired['alive'],expired
        rules={x['type']:x['metrics'] for x in done['rules']}
        assert rules['DUPLICATE']['duplicateGroups']==50000 and rules['DUPLICATE']['excessRows']==50000,rules
        assert rules['MISSING']['blankCount']==25000,rules
        assert p.service('backend')==backend
        assert e2e.http('/tasks/'+old['id'])[1]==old,'Existing task detail changed'
        report['checks'].append('lease expiry drains 100000-row real Spark job, then exits with no active tasks')
        report.update(ok=True,syntheticTask=task['id'],syntheticDataset=big['id'],metrics=rules,
          backendContainerUnchanged=True,expiredSession=expired)
        path=ROOT/'.runtime'/('operations-acceptance-'+uuid.uuid4().hex[:12]+'.json')
        st.save_json(path,report);print(json.dumps(dict(report,receipt=str(path)),ensure_ascii=False),flush=True)
    finally:
        # Never stop a Docker container, cancel a job, or remove a volume here.
        end=time.monotonic()+330
        while any(process.poll() is None for process in processes) and time.monotonic()<end:
            if rt.active(p)==0:
                with __import__('contextlib').suppress(st.StorageError):rt.stop()
            time.sleep(1)
        for process in processes:
            if process.poll() is not None:process.communicate(timeout=5)

if __name__=='__main__':main()
