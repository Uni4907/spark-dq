#!/usr/bin/env python3
"""Real MySQL/application migration checks. Only newly created isolated projects.

Usage: migrations_acceptance.py --legacy-backup PATH [--fixture-project PATH]
The fixture project must have a fresh isolated MySQL and an emptydb database.
All new volumes and failure evidence are retained; containers stop in finally.
"""
import argparse,hashlib,json,os,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT/'infrastructure'))
import storage as st
import e2e

def hashes(project):
    return {table:hashlib.sha256(project.mysql('SELECT * FROM '+table+' ORDER BY 1,2;')).hexdigest()
            for table in st.TABLES}

def history(project):
    return project.rows("SELECT JSON_OBJECT('rank',installed_rank,'version',version,'type',type,'checksum',checksum,'success',success) FROM flyway_schema_history ORDER BY installed_rank;")

def health(project):
    address=st.run(project.compose+['port','backend','8080']).decode().strip()
    assert address.startswith('127.0.0.1:'),address
    return 'http://'+address+'/api'

def start(project,folder,label,success=True,expected=None):
    st.run(project.compose+['up','-d','backend'])
    end=time.monotonic()+85;base=health(project)
    while time.monotonic()<end:
        state=st.run(['docker','inspect','--format','{{.State.Running}}',project.service('backend')]).strip()
        if state==b'false':break
        if success:
            try:
                with st.LOCAL.open(base+'/health',timeout=3) as response:
                    result=json.load(response)
                assert result['schemaVersion']=='2',result
                return base
            except OSError:pass
        time.sleep(1)
    # docker logs writes application output to both stdout and stderr.
    import subprocess
    log=subprocess.run(['docker','logs',project.service('backend')],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=15).stdout
    path=folder/(label+'.log');path.write_bytes(log);os.chmod(path,0o600)
    assert not success,'Backend startup failed; see '+str(path)
    assert state==b'false','Failed migration did not stop startup'
    assert expected.encode() in log,'Expected migration refusal absent: '+expected
    return base

def stop(project):
    st.run(project.compose+['stop','backend'],timeout=60)

def marker(project):
    dataset,task=str(uuid.uuid4()),str(uuid.uuid4())
    project.mysql("INSERT INTO datasets(id,name,file_path,sha256,size_bytes,row_count,columns_json,preview_json) VALUES ('"+dataset+"','migration-marker','/data/raw/marker.csv',REPEAT('0',64),0,0,JSON_ARRAY('id'),JSON_ARRAY());"+
      "INSERT INTO tasks(id,dataset_id,status) VALUES ('"+task+"','"+dataset+"','QUEUED');")
    return task

def untouched(project,task):
    assert project.mysql("SELECT status FROM tasks WHERE id='"+task+"';").strip()==b'QUEUED','Recovery ran before migration validation'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture-project',type=Path)
    parser.add_argument('--legacy-backup',type=Path,required=True)
    args=parser.parse_args()
    from migration_fixture import create_fixture
    folder=args.fixture_project.resolve() if args.fixture_project else create_fixture()
    assert folder.parent==ROOT/'.runtime' and folder.name.startswith('migrations-')
    config=folder/'compose.yaml';env=folder/'.env';reference=st.Project(config,env)
    identity=json.loads((folder/'setup.json').read_text())['project']
    assert identity.startswith('spark-dq-migrations-')
    labels=json.loads(st.run(['docker','inspect','--format','{{json .Config.Labels}}',reference.service('mysql')]))
    assert labels.get('com.docker.compose.project')==identity,'Fixture must point to its own isolated project'
    assert reference.mysql((ROOT/'backend/src/main/resources/db/legacy-metadata.sql').read_text()).decode()==(ROOT/'backend/src/main/resources/db/legacy-metadata.txt').read_text()
    projects=[];report={'checks':[],'fixture':str(folder)}
    def case(name):
        reference.mysql('CREATE DATABASE IF NOT EXISTS '+name+"; GRANT ALL ON "+name+".* TO 'sparkdq'@'%';")
        path=folder/(name+'.yaml')
        path.write_text(config.read_text().replace('SPARK_LOCAL_IP: 127.0.0.1','SPARK_LOCAL_IP: 127.0.0.1\n      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/'+name+'?connectionTimeZone=UTC&characterEncoding=UTF-8'))
        p=st.Project(path,env)
        # Project.mysql defaults to MYSQL_DATABASE. Target only this synthetic schema.
        original=p.mysql
        p.mysql=lambda sql=None,**kwargs:original('USE '+name+';\n'+sql if isinstance(sql,str) else sql,**kwargs)
        projects.append(p);return p,path
    try:
        p,_=case('emptydb');base=start(p,folder,'empty')
        rows=history(p);assert [(x['version'],x['type'],x['success']) for x in rows]==[('1','SQL',1),('2','SQL',1)],rows
        assert len(p.mysql('SHOW TABLES;').decode().splitlines())==13
        assert p.mysql("SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND INDEX_NAME IN ('tasks_status_created_id','datasets_created_id');").strip()==b'5'
        before=hashes(p);stop(p);start(p,folder,'repeat')
        assert history(p)==rows and hashes(p)==before
        report['checks'].append('empty database: V1/V2, indexes, schema health, repeat startup unchanged')
        stop(p)

        partial,_=case('partialdb')
        partial.mysql(';'.join((ROOT/'tests/fixtures/legacy-schema.sql').read_text().split(';')[:2])+';')
        task=marker(partial)
        start(partial,folder,'partial',False,'automatic baseline refused');untouched(partial,task)
        assert b'flyway_schema_history' not in partial.mysql('SHOW TABLES;')
        report['checks'].append('partial legacy schema refused before baseline and task recovery')

        changed,_=case('changeddb')
        changed.mysql((ROOT/'tests/fixtures/legacy-schema.sql').read_text())
        changed.mysql('ALTER TABLE tasks MODIFY error MEDIUMTEXT;')
        task=marker(changed);start(changed,folder,'changed',False,'automatic baseline refused');untouched(changed,task)
        report['checks'].append('changed legacy column type refused before task recovery')

        checksum,_=case('checksumdb')
        checksum.mysql((ROOT/'tests/fixtures/legacy-schema.sql').read_text())
        start(checksum,folder,'checksum-initial');stop(checksum);task=marker(checksum)
        checksum.mysql("UPDATE flyway_schema_history SET checksum=checksum+1 WHERE version='2';")
        start(checksum,folder,'checksum',False,'checksum mismatch');untouched(checksum,task)
        report['checks'].append('checksum mismatch stops startup and preserves QUEUED task')

        future,_=case('futuredb')
        future.mysql((ROOT/'tests/fixtures/legacy-schema.sql').read_text())
        start(future,folder,'future-initial');stop(future);task=marker(future)
        future.mysql("INSERT INTO flyway_schema_history(installed_rank,version,description,type,script,checksum,installed_by,execution_time,success) VALUES(3,'999','future','SQL','V999__future.sql',1,'acceptance',0,1);")
        start(future,folder,'future',False,'not resolved locally');untouched(future,task)
        report['checks'].append('future schema version blocks older application startup')

        fault,path=case('faultdb');fault.mysql((ROOT/'tests/fixtures/legacy-schema.sql').read_text())
        start(fault,folder,'fault-initial');stop(fault);task=marker(fault)
        faults=folder/'faults';faults.mkdir()
        (faults/'V3__deliberate_failure.sql').write_text('CREATE TABLE migration_failure_probe(id INT PRIMARY KEY);\nINSERT INTO deliberately_missing_table VALUES(1);\n')
        path.write_text(path.read_text().replace('SPARK_LOCAL_IP: 127.0.0.1','SPARK_LOCAL_IP: 127.0.0.1\n      SPRING_FLYWAY_LOCATIONS: classpath:db/migration,filesystem:/faults').replace('volumes: ["files:/data"]','volumes: ["files:/data", "'+str(faults)+':/faults:ro"]'))
        start(fault,folder,'ddl-failure',False,'Migration of schema');untouched(fault,task)
        assert history(fault)[-1]['success']==0
        failed=history(fault)
        start(fault,folder,'ddl-retry',False,'failed migration');untouched(fault,task)
        assert history(fault)==failed
        report['checks'].append('DDL failure retained as failed; repeated startup refuses without automatic repair/clean')
        stop(fault)

        # Protected pre-migration backup is restored to NEW volumes using its archived old app.
        clone=folder/'legacy-restore'
        receipt=st.restore(args.legacy_backup.resolve(),clone)
        old=st.Project(clone/'compose.yaml',clone/'.env');projects.append(old);stop(old)
        legacy_before=hashes(old);files_before=st.volume_inventory(old.volume('backend','/data'))
        upgraded=clone/'upgraded.yaml'
        old_image=st.verify(args.legacy_backup.resolve())['backendImageId']
        upgraded.write_text((clone/'compose.yaml').read_text().replace('image: '+old_image,'image: spark-dq-backend:migrations-v2').replace('volumes: [files:/data, application:/app:ro]','volumes: [files:/data]'))
        new=st.Project(upgraded,clone/'.env');projects.append(new)
        base=start(new,folder,'legacy-upgrade')
        assert hashes(new)==legacy_before,'Existing business rows changed during migration'
        assert st.volume_inventory(new.volume('backend','/data'))==files_before,'Existing data files changed during migration'
        rows=history(new);assert [(x['version'],x['type'],x['success']) for x in rows]==[('1','BASELINE',1),('2','SQL',1)],rows
        stop(new);base=start(new,folder,'legacy-repeat');assert history(new)==rows and hashes(new)==legacy_before
        # Real Spark rerun proves archived input + rules/results remain usable after upgrade.
        e2e.BASE=base
        source=new.mysql("SELECT t.id FROM tasks t JOIN task_snapshots s ON s.task_id=t.id LEFT JOIN governance_runs g ON g.task_id=t.id WHERE t.status='SUCCEEDED' AND g.task_id IS NULL ORDER BY t.created_at DESC,t.id DESC LIMIT 1;").decode().strip()
        assert source,'Backup needs a completed detection task with a rule snapshot for the Spark acceptance check'
        code,original=e2e.http('/tasks/'+source);assert code==200 and original['status']=='SUCCEEDED'
        code,created=e2e.http('/tasks/'+source+'/rerun',b'{}',{'Content-Type':'application/json'});assert code==200,created
        rerun=e2e.wait(created['id']);assert rerun['status']=='SUCCEEDED',rerun
        assert [(x['type'],x['columns'],x['metrics']) for x in rerun['rules']]==[(x['type'],x['columns'],x['metrics']) for x in original['rules']]
        stop(new);audit=st.audit(new);assert audit['ok'],audit
        # Back up and restore the NEW history-bearing layout too.
        new_backup=folder/'migrated-backup';manifest=st.backup(new,new_backup)
        assert manifest['counts']['flyway_schema_history']==2
        second=st.restore(new_backup,folder/'migrated-restore')
        second_project=st.Project(Path(second['config']),folder/'migrated-restore/.env');projects.append(second_project)
        assert history(second_project)==rows
        report.update(legacyProject=receipt['project'],legacyCounts=st.verify(args.legacy_backup.resolve())['counts'],
          rerunTask=rerun['id'],rerunMetrics=[x['metrics'] for x in rerun['rules']],migratedRestore=second['project'])
        report['checks'].append('real legacy backup: rows/files unchanged, baseline1 + V2, repeat, actual Spark rerun')
        report['checks'].append('new 13-table backup and isolated restore preserve migration history')
        report['ok']=True;st.save_json(folder/'acceptance.json',report)
        print(json.dumps(report,ensure_ascii=False),flush=True)
    finally:
        # All are disposable isolated containers; leave EVERY volume and failed schema intact.
        for project in reversed(projects):
            try:st.run(project.compose+['stop'],timeout=90)
            except st.StorageError:pass
        try:st.run(reference.compose+['stop'],timeout=90)
        except st.StorageError:pass

if __name__=='__main__':main()
