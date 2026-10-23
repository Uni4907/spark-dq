#!/usr/bin/env python3
"""Real isolated maintenance/backup/restart/failure/restore acceptance."""
import argparse,contextlib,json,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT/'infrastructure'))
import storage as st
import maintenance as m
import e2e
from migrations_acceptance import hashes

def rejected(control,action,owner):
    try:control.api(action,owner)
    except st.StorageError as error:assert 'HTTP 409' in str(error),str(error)
    else:raise AssertionError('Expected conflicting maintenance request')

def cli(project,output,folder,label):
    command=['python3',str(ROOT/'infrastructure/maintenance.py'),'--config',project.compose[5],'--env',project.compose[3],'backup','--output',str(output)]
    result=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=480)
    (folder/(label+'.log')).write_bytes(result.stdout)
    return result.returncode

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--backup',type=Path,required=True)
    args=parser.parse_args();backup=args.backup.resolve()
    folder=ROOT/'.runtime'/('maintenance-'+uuid.uuid4().hex[:12]);folder.mkdir(mode=0o700)
    source=st.Project();source_container=source.service('backend')
    projects=[];report={'folder':str(folder),'checks':[]}
    try:
        restored=st.restore(backup,folder/'clone')
        old=st.Project(Path(restored['config']),folder/'clone/.env');projects.append(old)
        st.run(old.compose+['stop','backend'],timeout=60)
        path=folder/'clone/candidate.yaml'
        image=st.verify(backup)['backendImageId']
        path.write_text(Path(restored['config']).read_text().replace('image: '+image,'image: spark-dq-backend:maintenance').replace('volumes: [files:/data, application:/app:ro]','volumes: [files:/data]'))
        project=st.Project(path,folder/'clone/.env');projects.append(project)
        before=hashes(project);files_before=st.volume_inventory(old.volume('backend','/data'))
        st.run(project.compose+['up','-d','--no-deps','backend'],timeout=100)
        control=m.Control(project);control.wait();assert not control.api()['maintenance']
        assert hashes(project)==before and st.volume_inventory(project.volume('backend','/data'))==files_before
        container=project.service('backend');e2e.BASE=control.address()
        assert e2e.http('/internal/maintenance')[0]==403
        assert e2e.http('/internal/maintenance',headers={'X-SparkDQ-Maintenance-Key':'wrong'})[0]==403
        report['checks'].append('candidate starts with rows/files unchanged and rejects unauthenticated control')

        code,data=e2e.upload((ROOT/'datasets/known-issues.csv').read_bytes(),'maintenance-synthetic.csv');assert code==200
        task=e2e.create(data['id'],['name','email'],['id'])
        rejected(control,'/enter',str(uuid.uuid4()))
        assert not control.api()['maintenance']
        done=e2e.wait(task['id']);assert done['status']=='SUCCEEDED',done
        report['checks'].append('busy Spark rejects maintenance without cancelling the job')

        owner=str(uuid.uuid4());control.api('/enter',owner)
        for method,url in [('POST','/tasks'),('POST','/tasks/'+task['id']+'/rerun'),('POST','/tasks/'+task['id']+'/govern'),('POST','/tasks/'+task['id']+'/cancel'),('PUT','/rule-templates/'+str(uuid.uuid4())),('DELETE','/rule-templates/'+str(uuid.uuid4()))]:
            assert e2e.http(url,b'{}',{'Content-Type':'application/json'},method)[0]==503,(method,url)
        assert e2e.upload(b'id\n1\n','maintenance-blocked.csv')[0]==503
        assert e2e.http('/tasks/'+task['id'])[0]==200
        assert e2e.http('/health')[1]['maintenance'] is True
        rejected(control,'/leave',str(uuid.uuid4()))
        assert control.api()['owner']==owner
        report['checks'].append('all mutation routes blocked while GET reads remain; foreign owner cannot unlock')

        interrupted=folder/'interrupted';interrupted.mkdir(mode=0o700)
        receipt={'formatVersion':'sparkdq-maintenance-v1','owner':owner,'backendContainerId':container,
          'compose':project.compose,'output':str(interrupted),'state':'offline','needsResume':True,
          'serviceResumed':False,'maintenanceReleased':False}
        receipt_path=interrupted/'receipt.json';m.persist(receipt_path,receipt)
        old_key=st.run(['docker','exec',container,'cat',m.KEY_PATH])
        st.run(project.compose+['stop','backend'],timeout=60)
        st.run(project.compose+['start','backend'],timeout=90);control.wait();e2e.BASE=control.address()
        assert project.service('backend')==container
        assert control.api()['owner']==owner and e2e.http('/tasks',b'{}',{'Content-Type':'application/json'})[0]==503
        assert st.run(['docker','exec',container,'cat',m.KEY_PATH])!=old_key
        recovered=m.resume(receipt_path)
        assert recovered['maintenanceReleased'] and not recovered['needsResume'] and recovered['state']=='recovered'
        assert not control.api()['maintenance']
        report['checks'].append('same-container restart keeps admission closed, rotates key, receipt resume safely releases')

        before=hashes(project);files_before=st.volume_inventory(project.volume('backend','/data'))
        good=folder/'successful'
        assert cli(project,good,folder,'successful')==0
        e2e.BASE=control.address()
        successful=json.loads((good/'receipt.json').read_text())
        assert successful['state']=='complete' and successful['serviceResumed'] and successful['maintenanceReleased']
        assert project.service('backend')==container and hashes(project)==before
        assert st.volume_inventory(project.volume('backend','/data'))==files_before
        assert not control.api()['maintenance'] and e2e.http('/health')[1]['maintenance'] is False
        manifest=st.verify(good/'backup');assert manifest['counts']['flyway_schema_history']==2
        report['checks'].append('one-command gated backup verifies and resumes same container with identical rows/files')

        second=st.restore(good/'backup',folder/'restored-backup')
        second_project=st.Project(Path(second['config']),folder/'restored-backup/.env');projects.append(second_project)
        second_control=m.Control(second_project)
        assert not second_control.api()['maintenance'],'Temporary maintenance marker leaked into backup'
        e2e.BASE=second_control.address()
        code,created=e2e.http('/tasks/'+task['id']+'/rerun',b'{}',{'Content-Type':'application/json'});assert code==200,created
        rerun=e2e.wait(created['id']);assert rerun['status']=='SUCCEEDED',rerun
        assert [r['metrics'] for r in rerun['rules']]==[r['metrics'] for r in done['rules']]
        report['checks'].append('isolated restore opens normally without maintenance key/marker and real Spark rerun matches')

        # Kill ONLY this disposable backup coordinator after its offline receipt is durable.
        killed=folder/'killed-coordinator'
        command=['python3',str(ROOT/'infrastructure/maintenance.py'),'--config',project.compose[5],
          '--env',project.compose[3],'backup','--output',str(killed)]
        with (folder/'killed-coordinator.log').open('wb') as log:
            process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
            end=time.monotonic()+50;observed=False
            while time.monotonic()<end and process.poll() is None:
                try:
                    saved=json.loads((killed/'receipt.json').read_text())
                    if saved['state']=='offline':
                        process.kill();observed=True;break
                except (OSError,ValueError):pass
                time.sleep(.01)
            if not observed:
                process.wait(timeout=450)
                raise AssertionError('Could not observe offline phase for isolated interruption test')
            assert process.wait(timeout=15)<0
        assert not project.running('backend')
        with st.lock():resumed=m.resume(killed/'receipt.json')
        assert resumed['state']=='recovered' and resumed['maintenanceReleased']
        assert not (killed/'backup/COMPLETE').exists()
        e2e.BASE=control.address()
        assert e2e.http('/health')[1]['maintenance'] is False
        report['checks'].append('actual coordinator SIGKILL leaves offline receipt; lock releases and resume restores same writable service')

        # Add one missing-file probe ONLY to the disposable candidate clone.
        e2e.BASE=control.address();probe=str(uuid.uuid4())
        project.mysql("INSERT INTO datasets(id,name,file_path,sha256,size_bytes,row_count,columns_json,preview_json) VALUES('"+probe+"','missing-file-probe','/data/raw/missing-maintenance-probe.csv',REPEAT('0',64),1,1,JSON_ARRAY('id'),JSON_ARRAY());")
        before=hashes(project)
        failed=folder/'failed-audit'
        assert cli(project,failed,folder,'failed-audit')!=0
        e2e.BASE=control.address()
        failure=json.loads((failed/'receipt.json').read_text())
        assert failure['state']=='failed' and failure['serviceResumed'] and failure['maintenanceReleased'] and not failure['needsResume']
        assert not (failed/'backup/COMPLETE').exists()
        assert project.service('backend')==container and hashes(project)==before and not control.api()['maintenance']
        assert e2e.upload(b'id\n1\n','after-failed-maintenance.csv')[0]==200
        report['checks'].append('real missing-file audit failure retains incomplete backup and restores writable service')
        assert source.service('backend')==source_container
        report.update(ok=True,successfulReceipt=str(good/'receipt.json'),interruptedReceipt=str(receipt_path),
          failedReceipt=str(failed/'receipt.json'),rerunTask=rerun['id'],sourceContainerUnchanged=True)
        st.save_json(folder/'acceptance.json',report);print(json.dumps(report,ensure_ascii=False),flush=True)
    finally:
        for project in reversed(projects):
            with contextlib.suppress(st.StorageError):st.run(project.compose+['stop'],timeout=90)

if __name__=='__main__':main()
