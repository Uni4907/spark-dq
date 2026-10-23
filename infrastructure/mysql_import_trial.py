#!/usr/bin/env python3
"""Bounded, isolated formal-API acceptance. Never starts or changes the primary project."""
import collections,contextlib,csv,hashlib,io,json,os,secrets,select,shutil,subprocess,sys,tarfile,time,traceback,urllib.request,urllib.error,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'infrastructure')]
import cluster_lab,storage
from iqr_acceptance import Client,csv_bytes,digest,LOCAL
from algorithm_oracle import iqr,fd

def run(args,**kw):return subprocess.run(args,check=True,timeout=kw.pop('timeout',120),**kw)
def output(args):return subprocess.check_output(args,timeout=40,text=True).strip()
def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2))
def request(c,path,body=None,headers=None):
    raw=None if body is None else json.dumps(body,ensure_ascii=False).encode()
    req=urllib.request.Request(c.base+path,data=raw,headers=dict({'Content-Type':'application/json'},**(headers or {})))
    try:
        with LOCAL.open(req,timeout=35) as res:return res.status,json.load(res)
    except urllib.error.HTTPError as error:return error.code,json.load(error)
def wait_import(c,identity,expected=None):
    deadline=time.monotonic()+50
    while time.monotonic()<deadline:
        code,row=c.call('/mysql-imports/'+identity);assert code==200
        if row['status'] in ('SUCCEEDED','FAILED','CANCELLED'):
            if expected:assert row['status']==expected,(identity,row['status'],row.get('error'))
            return row
        time.sleep(.1)
    raise TimeoutError('Import did not reach terminal state')
def submit(c,source,fields,key=None):
    key=key or str(uuid.uuid4());code,row=request(c,'/mysql-imports',{'sourceId':source,'columns':fields},{'X-Import-Key':key});assert code==202,(code,row)
    return row,key
def ready(c):
    deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        try:
            if c.call('/health')[1]['status']=='UP':return
        except Exception:pass
        time.sleep(1)
    raise TimeoutError('Candidate health')
def full_rows(backend,task,root):
    raw=subprocess.check_output(['docker','cp',backend+':/data/tasks/'+task['id']+'/anomalies','-'],timeout=45)
    (root/(task['id']+'-anomalies.tar')).write_bytes(raw);result={r['id']:[] for r in task['rules']}
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for member in archive:
            if not member.isfile() or not Path(member.name).name.startswith('part-') or not member.name.endswith('.json'):continue
            identity=Path(member.name).parent.name;assert identity in result
            for line in archive.extractfile(member):
                if line.strip():result[identity].append(json.loads(line))
    return result
def multiset(rows):return collections.Counter(json.dumps(row,sort_keys=True,ensure_ascii=False,separators=(',',':')) for row in rows)
def signature(rule):return json.dumps({k:rule.get(k) for k in ('type','columns','params')},sort_keys=True)
def validate_task(c,backend,task,rows,root):
    actual=full_rows(backend,task,root);save(root/(task['id']+'.json'),task)
    save(root/(task['id']+'-report.json'),c.call('/tasks/'+task['id']+'/report')[1]);(root/(task['id']+'-report.html')).write_bytes(c.raw('/tasks/'+task['id']+'/report.html'))
    for rule in task['rules']:
        if rule['type']=='IQR':expected,indexes=iqr([r[rule['columns'][0]] for r in rows],rule['params']['k'])
        elif rule['type']=='FD':expected,indexes=fd(rows,rule['columns'],rule['params']['dependentColumns'])
        else:continue
        assert rule['metrics']==expected,(rule['type'],rule['metrics'],expected)
        assert multiset(actual[rule['id']])==multiset([rows[i] for i in indexes]),rule['type']
    return {signature(r):{'metrics':r['metrics'],'anomalies':multiset(actual[r['id']])} for r in task['rules']}
def main():
    if '--prepared' in sys.argv:
        root=Path(sys.argv[sys.argv.index('--prepared')+1]).resolve()
        assert root.parent==ROOT/'.runtime' and root.name.startswith('t1-mysql-http-')
        lab=json.loads((root/'lab.json').read_text());candidate=lab['candidate']
        reader=(root/'reader.password').read_text();wrong=(root/'wrong.password').read_text()
        fields=['id','金额','numeric','x1','x2','y1','y2','text','day'];targets=['amount' if x=='金额' else x for x in fields]
        env=root/'.env';compose=root/'compose.yaml';command=['docker','compose','--env-file',str(env),'-f',str(compose)]
    else:
        candidate=json.loads(sorted((ROOT/'.runtime').glob('t1-build-*/candidate.json'))[-1].read_text());tag=candidate['backendImage']
        root=ROOT/'.runtime'/('t1-mysql-http-'+time.strftime('%Y%m%d-%H%M%S')+'-'+secrets.token_hex(3));root.mkdir(mode=0o700)
        assert shutil.disk_usage('/mnt/c').free>=8*1024**3 and shutil.disk_usage(root).free>=8*1024**3
        available=int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:')))*1024;assert available>=4*1024**3
        project='sparkdq-mysql-'+secrets.token_hex(6);plan=cluster_lab.network_plan();reader=secrets.token_hex(24);wrong=secrets.token_hex(24)
        env=root/'.env';env.write_text('MYSQL_PASSWORD='+secrets.token_hex(24)+'\nMYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\nBUSINESS_ROOT_PASSWORD='+secrets.token_hex(24)+'\n');env.chmod(0o600)
        for name,password in [('reader.password',reader),('wrong.password',wrong)]:
            path=root/name;path.write_text(password);os.chown(path,1000,1000);path.chmod(0o400)
        fields=['id','金额','numeric','x1','x2','y1','y2','text','day'];targets=['amount' if x=='金额' else x for x in fields]
        definitions=[]
        for name,limits in [('fixture',{}),('row_limit',{'maxRows':2}),('byte_limit',{'maxBytes':64}),('timeout',{'timeoutSeconds':1}),('wrong',{})]:
            definitions.append(dict(id=name,host='business',port=3306,user='snapshot_reader',tlsMode='DISABLED',passwordFile='/run/'+('wrong' if name=='wrong' else 'reader')+'.password',schema='business_fixture',table='sample',columns=[{'source':s,'target':t} for s,t in zip(fields,targets)],**limits))
        config=root/'sources.json';save(config,{'sources':definitions});os.chown(config,1000,1000);config.chmod(0o400)
        template=(ROOT/'infrastructure/compose.yaml').read_text().replace('name: spark-dq','name: '+project)
        template=template.replace('spark-dq-backend:0.1.0',tag).replace('spark-dq-frontend:0.1.0','spark-dq-frontend:b3-algorithms-20261021')
        for role in ('backend','frontend'):
            template=template.replace('    build:\n      context: ..\n      dockerfile: infrastructure/'+role+'.Dockerfile\n','')
        template=template.replace('127.0.0.1:18080:8080','127.0.0.1::8080').replace('127.0.0.1:15173:5173','127.0.0.1::5173')
        template=template.replace('\n  backend:\n','\n  backend:\n    mem_limit: 3g\n    cpus: 2\n').replace('\n  mysql:\n','\n  mysql:\n    mem_limit: 768m\n').replace('\n  frontend:\n','\n  frontend:\n    mem_limit: 256m\n')
        template=template.replace('      SPARK_LOCAL_IP: 127.0.0.1','      SPARK_LOCAL_IP: 127.0.0.1\n      SPARK_DQ_MYSQL_SOURCES_FILE: /run/mysql-sources.json')
        template=template.replace('      - files:/data','      - files:/data\n      - '+str(config)+':/run/mysql-sources.json:ro\n      - '+str(root/'reader.password')+':/run/reader.password:ro\n      - '+str(root/'wrong.password')+':/run/wrong.password:ro')
        business='\n'.join(['  business:','    image: '+storage.MYSQL_IMAGE,'    mem_limit: 768m','    cpus: 1','    environment:','      MYSQL_ROOT_PASSWORD: ${BUSINESS_ROOT_PASSWORD}','    volumes:','      - business-data:/var/lib/mysql',''])
        template=template.replace('\n  backend:\n','\n'+business+'  backend:\n').replace('\n  files:\n','\n  files:\n  business-data:\n')
        template+='\nnetworks:\n  default:\n    ipam:\n      config:\n        - subnet: '+plan['default']+'\n'
        compose=root/'compose.yaml';compose.write_text(template);command=['docker','compose','--env-file',str(env),'-f',str(compose)]
        save(root/'lab.json',{'project':project,'candidate':candidate,'networkPlan':plan,'resourceAdmission':{'hostFreeBytes':shutil.disk_usage('/mnt/c').free,'wslAvailableMemory':available},'businessPublicPorts':[],'mainDeploymentTouched':False})
    restore_project=None;lock=None;summary={'ok':False,'root':str(root),'candidate':candidate,'checks':[]}
    def check(name):summary['checks'].append(name);save(root/'progress.json',summary);print(name,flush=True)
    if '--prepare' in sys.argv:
        run(command+['config','--quiet']);print(json.dumps({'preparedRoot':str(root),'project':project,'backendImage':tag,'noContainersStarted':True}),flush=True);return root
    try:
        with (root/'startup.log').open('x') as log:run(command+['up','-d','--no-build','--pull','never'],stdout=log,stderr=subprocess.STDOUT,timeout=180)
        backend=output(command+['ps','-q','backend']);business_id=output(command+['ps','-q','business']);c=Client('http://'+output(command+['port','backend','8080'])+'/api');ready(c)
        def sql(statement):return subprocess.check_output(['docker','exec','-i',business_id,'sh','-c','export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; exec mysql -u root --binary-mode --default-character-set=utf8mb4 --batch --raw --skip-column-names'],input=statement.encode(),timeout=40).decode()
        deadline=time.monotonic()+100
        while time.monotonic()<deadline:
            try:sql('SELECT 1;');break
            except subprocess.CalledProcessError:time.sleep(1)
        else:raise TimeoutError('Business fixture readiness')
        sql("CREATE DATABASE business_fixture CHARACTER SET utf8mb4; CREATE TABLE business_fixture.sample(`id` BIGINT UNSIGNED, `金额` DECIMAL(38,10), `numeric` VARCHAR(100),`x1` TEXT,`x2` TEXT,`y1` TEXT,`y2` TEXT,`text` TEXT,`day` DATE) ENGINE=InnoDB; CREATE USER 'snapshot_reader'@'%' IDENTIFIED BY '"+reader+"'; GRANT SELECT ON business_fixture.sample TO 'snapshot_reader'@'%';")
        fixture=json.loads((ROOT/'tests/algorithm-independent-cases.json').read_text())['fdRows'];rows=[];numeric=['1\n',' 1','2 ','1\t',None,'','   ','1.00000000001','NaN','0','1']
        for i,r in enumerate(fixture):rows.append(dict(id=str(9007199254740993+i),amount=str(9007199254740993+i)+'.0000000001',numeric=numeric[i%len(numeric)],x1=r.get('x1'),x2=r.get('x2'),y1=r.get('y1'),y2=r.get('y2'),text=['中文\r\n换行\t引号"',None,'',' '][i%4],day='2026-10-21' if i%2 else None))
        rows.extend([dict(rows[0]),dict(rows[2])]);rows[-1]['amount']='9007199254741993.0000000001'
        def quote(v):return 'NULL' if v is None else "'"+v.replace('\\','\\\\').replace("'","''")+"'"
        sql('INSERT INTO business_fixture.sample VALUES '+','.join('('+','.join(quote(row[t]) for t in targets)+')' for row in rows)+';')
        save(root/'fixture-rows.json',rows);assert request(c,'/mysql-sources')[0]==200
        views=c.call('/mysql-sources')[1];assert len(views)==5;assert reader not in json.dumps(views) and 'passwordFile' not in json.dumps(views)
        assert request(c,'/mysql-imports',{'sourceId':'fixture','columns':['not_allowed']},{'X-Import-Key':str(uuid.uuid4())})[0]==400
        first,key=submit(c,'fixture',fields);first=wait_import(c,first['id'],'SUCCEEDED');same,_=submit(c,'fixture',fields,key);assert same['id']==first['id']
        assert request(c,'/mysql-imports',{'sourceId':'fixture','columns':fields[:-1]},{'X-Import-Key':key})[0]==409
        data=c.call('/datasets/'+first['dataset_id'])[1];before=c.raw('/datasets/'+data['id']+'/download');assert c.raw('/datasets/'+data['id']+'/sources/1/download')==before
        (root/'mysql-snapshot.csv').write_bytes(before)
        decoded=[{k:None if v=='\\N' else v for k,v in row.items()} for row in csv.DictReader(io.StringIO(before.decode(),newline=''))]
        assert multiset(decoded)==multiset(rows),'Source snapshot values differ from original fixture'
        imported=c.upload(csv_bytes(targets,rows),'matching-logical-data.csv');check('formal API whitelist/idempotence/source download')
        rules=[{'type':'IQR','columns':['amount']},{'type':'IQR','columns':['numeric']},{'type':'FD','columns':['x1','x2'],'params':{'dependentColumns':['y1','y2']}},{'type':'MISSING','columns':['text']},{'type':'DUPLICATE','columns':['x1','x2']},{'type':'TYPE','columns':['day'],'params':{'dataType':'DATE'}},{'type':'RANGE','columns':['amount'],'params':{'min':'9007199254740993','max':'9007199254741093'}},{'type':'REGEX','columns':['text'],'params':{'pattern':'.*'}}]
        code,template=c.call('/rule-templates',{'name':'MySQL业务快照正式验收','rules':rules});assert code==200
        t1=c.task({'datasetId':data['id'],'templateId':template['template_id'],'templateRevision':1});expected=validate_task(c,backend,t1,rows,root)
        t2=c.task({'datasetId':imported['id'],'rules':rules});assert validate_task(c,backend,t2,rows,root)==expected;check('CSV/MySQL all metrics, exact IQR thresholds and complete anomaly multisets; Decimal/FD oracles')
        changed=json.loads(json.dumps(rules));changed[0]['params']={'k':'2'};assert c.call('/rule-templates/'+template['template_id'],{'name':'更新不改变旧快照','rules':changed,'expectedRevision':1},'PUT')[0]==200
        sql('UPDATE business_fixture.sample SET 金额=金额+100000;');fresh,_=submit(c,'fixture',fields);fresh=wait_import(c,fresh['id'],'SUCCEEDED');assert fresh['dataset_id']!=data['id'];fresh_data=c.call('/datasets/'+fresh['dataset_id'])[1];assert fresh_data['sha256']!=data['sha256'];assert c.raw('/datasets/'+data['id']+'/download')==before
        replay=c.task({},'/tasks/'+t1['id']+'/rerun');assert validate_task(c,backend,replay,rows,root)==expected;check('source mutation/new fingerprint, frozen template and original-rule rerun')
        for alias,error in [('wrong','SNAPSHOT_FAILED'),('row_limit','ROW_LIMIT'),('byte_limit','BYTE_LIMIT')]:
            job,_=submit(c,alias,fields);job=wait_import(c,job['id'],'FAILED');assert job['error']==error,(alias,job['error']);assert job['dataset_id'] is None;assert c.call('/datasets/'+job['id'])[0]==404;save(root/(alias+'-failure.json'),job)
        check('invalid credentials and row/byte limits never publish a dataset')
        def acquire_lock():
            proc=subprocess.Popen(['docker','exec','-i',business_id,'sh','-c','export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; exec mysql -u root --batch --raw --skip-column-names --unbuffered'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            proc.stdin.write(b"LOCK TABLES business_fixture.sample WRITE; SELECT 'LOCKED';\n");proc.stdin.flush();assert select.select([proc.stdout],[],[],15)[0];assert proc.stdout.readline().strip()==b'LOCKED';return proc
        def release_lock(proc):
            if proc is not None:
                try:proc.stdin.write(b'UNLOCK TABLES; quit\n');proc.stdin.close();proc.wait(timeout=15)
                except Exception:proc.kill()
        lock=acquire_lock();job,_=submit(c,'timeout',fields);job=wait_import(c,job['id'],'FAILED');assert job['error']=='TIMEOUT';save(root/'timeout-failure.json',job)
        live,_=submit(c,'fixture',fields)
        deadline=time.monotonic()+5
        while c.call('/mysql-imports/'+live['id'])[1]['status']!='RUNNING':assert time.monotonic()<deadline;time.sleep(.1)
        queued,_=submit(c,'fixture',fields);assert queued['status']=='QUEUED';assert c.call('/mysql-imports/'+queued['id']+'/cancel',{})[0]==200;wait_import(c,queued['id'],'CANCELLED')
        control_key=output(['docker','exec',backend,'cat','/tmp/sparkdq-maintenance.key']);owner=str(uuid.uuid4());headers={'X-SparkDQ-Maintenance-Key':control_key}
        assert request(c,'/internal/maintenance/enter',{'owner':owner},headers)[0]==409
        assert c.call('/mysql-imports/'+live['id']+'/cancel',{})[0]==200;wait_import(c,live['id'],'CANCELLED');release_lock(lock);lock=None
        assert request(c,'/internal/maintenance/enter',{'owner':owner},headers)[0]==200
        assert request(c,'/mysql-imports',{'sourceId':'fixture','columns':fields},{'X-Import-Key':str(uuid.uuid4())})[0]==503
        assert request(c,'/internal/maintenance/leave',{'owner':owner},headers)[0]==200;check('real timeout/cancel, queued cancel, maintenance busy/admission gates')
        lock=acquire_lock();interrupted,_=submit(c,'fixture',fields);time.sleep(.4);pending,_=submit(c,'fixture',fields)
        run(['docker','kill',backend],stdout=subprocess.DEVNULL);release_lock(lock);lock=None;run(['docker','start',backend],stdout=subprocess.DEVNULL)
        c=Client('http://'+output(command+['port','backend','8080'])+'/api');ready(c)
        for job in (interrupted,pending):assert wait_import(c,job['id'],'FAILED')['dataset_id'] is None
        assert c.raw('/datasets/'+data['id']+'/download')==before;check('backend interruption recovers running/queued imports without partial datasets')
        sql("UPDATE business_fixture.sample SET text='\\\\N';");bad,_=submit(c,'fixture',fields);bad=wait_import(c,bad['id'],'FAILED');assert bad['error']=='UNREPRESENTABLE_VALUE';save(root/'reserved-null-failure.json',bad)
        run(command+['stop','backend'],stdout=subprocess.DEVNULL);project_object=storage.Project(compose,env)
        saved=storage.backup(project_object,root/'backup');verified=storage.verify(root/'backup');save(root/'backup-result.json',saved);check('isolated backup verified including MYSQL_SNAPSHOT provenance')
        restored=storage.restore(root/'backup',root/'restored');restore_project=storage.Project(root/'restored/compose.yaml',root/'restored/.env');restored_backend=restore_project.service('backend');rc=Client(restored['api']);ready(rc)
        assert rc.call('/mysql-sources')[1]==[];assert rc.raw('/datasets/'+data['id']+'/download')==before;assert rc.raw('/datasets/'+data['id']+'/sources/1/download')==before
        rt=rc.task({},'/tasks/'+t1['id']+'/rerun');assert validate_task(rc,restored_backend,rt,rows,root)==expected;check('fresh-volume restore: source credentials absent, source download and original-rule rerun work')
        for filename in ('database.sql','files.tar','application.tar'):
            with (root/'backup'/filename).open('rb') as stream:
                tail=b''
                for block in iter(lambda:stream.read(1024*1024),b''):
                    combined=tail+block;assert reader.encode() not in combined and wrong.encode() not in combined;tail=combined[-64:]
        summary.update(ok=True,api=c.base,restoredApi=rc.base,sourceDataset=data['id'],newDataset=fresh_data['id'],csvDataset=imported['id'],tasks=[t1['id'],t2['id'],replay['id'],rt['id']],snapshotSha256=data['sha256'],backupRecoveryPoint=verified['recoveryPoint']['sha256'],completeMultisetChecked=True,sourceCredentialsInBackup=False)
    except Exception:
        (root/'failure.txt').write_text(traceback.format_exc());raise
    finally:
        if lock is not None:
            with contextlib.suppress(Exception):release_lock(lock)
        if restore_project is not None:
            with contextlib.suppress(Exception):run(restore_project.compose+['stop'],stdout=subprocess.DEVNULL,timeout=90)
        with contextlib.suppress(Exception):run(command+['stop'],stdout=subprocess.DEVNULL,timeout=90)
        summary['cleanup']='Owned candidate/restore containers stopped; all volumes/networks retained'
        save(root/'summary.json',summary);print(json.dumps(summary,ensure_ascii=False),flush=True)
    return root
if __name__=='__main__':main()
