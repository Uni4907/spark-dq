"""Real database/files backup -> fresh restore -> Spark recheck acceptance.

Requires project Docker permission and an idle maintenance window. Original
backend is stopped only for capture and resumed in finally. All clone volumes
are newly generated, stopped at the end and retained; no volume is deleted.
Only additive synthetic probes are inserted into the isolated clone.
Run: timeout 1500s python3 tests/storage_acceptance.py
"""
from pathlib import Path
import hashlib,json,os,subprocess,sys,time,urllib.error,urllib.request,uuid
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT/'infrastructure'))
import storage
sys.path.insert(0,str(ROOT/'tests'))
from e2e import upload
LOCAL=storage.LOCAL
SOURCE='http://127.0.0.1:18080/api'

def http(base,path,body=None,method=None,expected=200,raw=False):
    encoded=json.dumps(body,ensure_ascii=False).encode() if body is not None else None
    req=urllib.request.Request(base+path,data=encoded,method=method,headers={'Content-Type':'application/json'})
    try:
        with LOCAL.open(req,timeout=30) as response:return response.read() if raw else json.load(response)
    except urllib.error.HTTPError as error:
        if error.code==expected:return json.load(error)
        raise AssertionError((path,error.code)) from None

def healthy(base):
    end=time.monotonic()+90
    while time.monotonic()<end:
        try:
            if http(base,'/health')['status']=='UP':return
        except OSError:pass
        time.sleep(1)
    raise AssertionError('API health timeout')

def completed(base,id):
    end=time.monotonic()+310
    while time.monotonic()<end:
        task=http(base,'/tasks/'+id)
        if task['status']=='SUCCEEDED':return task
        assert task['status'] not in ('FAILED','CANCELLED'),task
        time.sleep(.5)
    raise AssertionError('Spark task timeout')

def cli(arguments,expected=0):
    result=subprocess.run([sys.executable,str(ROOT/'infrastructure/storage.py')]+[str(a) for a in arguments],
      cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=600)
    assert result.returncode==expected,(arguments[0],result.returncode,result.stderr.decode()[:1000])
    return json.loads(result.stdout) if result.stdout else {}

def check_idle():
    for status in ('QUEUED','RUNNING','CANCELLING'):
        assert http(SOURCE,'/tasks/page?status='+status)['total']==0,'Active task; defer maintenance'

def capture(base,id):
    task=http(base,'/tasks/'+id)
    result={'task':task,'report':http(base,'/tasks/'+id+'/report')}
    if task['status']=='SUCCEEDED':
        result['previews']={r['id']:http(base,'/tasks/'+id+'/rules/'+r['id']+'/preview') for r in task['rules']}
    return result

def main():
    source=storage.Project();clone=None;paused=False
    directory=ROOT/'.runtime'/('storage-'+uuid.uuid4().hex[:12]);directory.mkdir(mode=0o700)
    storage.save_json(ROOT/'.runtime'/('storage-pointer-'+directory.name+'.json'),{'run':str(directory)})
    original_env=storage.digest(ROOT/'.env')
    try:
        storage.run(source.compose+['up','-d']);healthy(SOURCE);check_idle()
        # Existing terminal history is compared exactly after restore.
        history=[]
        for status in ('SUCCEEDED','FAILED','CANCELLED'):
            page=http(SOURCE,'/tasks/page?status='+status+'&pageSize=1')
            if page['items']:history.append(capture(SOURCE,page['items'][0]['id']))
        raw=('id,name,age,code\r\n1,Alice,18,X01\r\n1,"   ",19," x02 "\r\n'
             '2,\\N,20,X03\r\n3,"中文""多行\n值",bad,X04\r\n4,Bob,200,BAD\r\n').encode()
        code,data=upload(raw,'storage-synthetic-'+directory.name+'.csv');assert code==200,data
        rules=[{'type':'MISSING','columns':['name']},{'type':'DUPLICATE','columns':['id']},
          {'type':'TYPE','columns':['age'],'params':{'dataType':'INTEGER'}},
          {'type':'RANGE','columns':['age'],'params':{'min':'0','max':'120'}},
          {'type':'REGEX','columns':['code'],'params':{'pattern':'X[0-9]{2}'}}]
        template=http(SOURCE,'/rule-templates',{'name':'storage-'+directory.name,'rules':rules},'POST')
        tid=template['template_id']
        baseline=completed(SOURCE,http(SOURCE,'/tasks',{'datasetId':data['id'],'templateId':tid,'templateRevision':1},'POST')['id'])
        governed=completed(SOURCE,http(SOURCE,'/tasks/'+baseline['id']+'/govern',
          {'fills':{'name':'待补齐'},'normalizations':{'code':'TRIM_UPPER'},'deduplicate':True},'POST')['id'])
        assert baseline['total_rows']==5 and governed['total_rows']==4
        http(SOURCE,'/rule-templates/'+tid,{'name':'storage-second-'+directory.name,'rules':rules,'expectedRevision':1},'PUT')
        templates=[http(SOURCE,'/rule-templates/'+tid+'?revision='+str(r)) for r in (1,2)]
        records=history+[capture(SOURCE,baseline['id']),capture(SOURCE,governed['id'])]
        dataset_ids=[data['id'],governed['governance']['output_dataset_id']]
        datasets=[http(SOURCE,'/datasets/'+id) for id in dataset_ids]
        downloads={id:http(SOURCE,'/datasets/'+id+'/download',raw=True) for id in dataset_ids}
        assert downloads[data['id']]==raw
        check_idle()
        # Running service refusal is checked before any report is created.
        refusal=directory/'online-refused.json'
        cli(['audit','--output',refusal],expected=1);assert not refusal.exists()
        storage.run(source.compose+['stop','backend'],timeout=90);paused=True
        result=cli(['audit','--output',directory/'source-audit.json'])
        assert result['ok'],result
        frozen=source.inventory()
        saved=cli(['backup','--output',directory/'backup'])
        assert saved['counts']==frozen['counts']
        cli(['verify',directory/'backup'])
        assert not (directory/'backup/.env').exists()
        assert all((p.stat().st_mode&0o777)==0o600 for p in (directory/'backup').iterdir())
        # Reusing even the successful output directory is refused without overwriting.
        before=storage.digest(directory/'backup/COMPLETE')
        cli(['backup','--output',directory/'backup'],expected=1)
        assert storage.digest(directory/'backup/COMPLETE')==before
        storage.run(source.compose+['up','-d','backend']);paused=False;healthy(SOURCE)
        print('PASS offline audit/backup: running backend refused; database + read-only files captured, counts/checksums verified, restricted permissions, original credentials excluded, existing destination preserved',flush=True)
        receipt=cli(['restore',directory/'backup','--output',directory/'restore'])
        clone=storage.Project(directory/'restore/compose.yaml',directory/'restore/.env')
        assert receipt['mysqlVolume']!=source.volume('mysql','/var/lib/mysql')
        assert receipt['filesVolume']!=source.volume('backend','/data')
        assert storage.digest(directory/'restore/.env')!=original_env
        base=receipt['api']
        for item in records:assert capture(base,item['task']['id'])==item
        for id,expected in zip(dataset_ids,datasets):
            assert http(base,'/datasets/'+id)==expected
            assert http(base,'/datasets/'+id+'/download',raw=True)==downloads[id]
        for revision,expected in enumerate(templates,1):
            assert http(base,'/rule-templates/'+tid+'?revision='+str(revision))==expected
        print('PASS isolated restore: all table counts and every archived file match; original/derived CSV, task/rule/result IDs, snapshots, audits, reports, anomaly previews and two template revisions retained',flush=True)
        rerun=completed(base,http(base,'/tasks/'+governed['id']+'/rerun',{},'POST')['id'])
        assert rerun['total_rows']==4 and rerun['template']['revision']==1
        keyed=lambda task:{(r['type'],tuple(r['columns']),json.dumps(r['params'],sort_keys=True)):r['metrics'] for r in task['rules']}
        assert keyed(rerun)==keyed(governed)
        assert rerun['snapshot']['parent_task_id']==governed['id']
        assert http(SOURCE,'/tasks/'+rerun['id'],expected=404)['message']
        assert source.inventory()['counts']==frozen['counts']
        print('PASS real Spark after restore: independent four-row recheck retains all five metrics and revision 1; clone writes do not appear in original database',flush=True)
        # Add brand-new probes only in the isolated clone. Never alter existing files.
        storage.run(clone.compose+['stop','backend'],timeout=90)
        volume=receipt['filesVolume']
        label=storage.run(['docker','volume','inspect','--format','{{ index .Labels "sparkdq.restore" }}',volume]).decode().strip()
        assert label==receipt['project']
        missing,changed,orphan=[str(uuid.uuid4()) for _ in range(3)]
        probe=b'id\nchanged\n'
        storage.run(['docker','run','--rm','--network','none','--read-only','--cap-drop','ALL',
          '--mount','type=volume,source='+volume+',target=/data','--entrypoint','sh',storage.BACKEND_IMAGE,
          '-c','set -eu; set -C; printf "id\\nchanged\\n" > "/data/raw/$1.csv"; printf "id\\norphan\\n" > "/data/raw/$2.csv"','sh',changed,orphan])
        for id,name,size in ((missing,'missing',1),(changed,'changed',len(probe))):
            clone.mysql("INSERT INTO datasets(id,name,file_path,sha256,size_bytes,row_count,columns_json,preview_json) VALUES ('"+
              id+"','storage-audit-probe-"+name+".csv','/data/raw/"+id+".csv','"+'0'*64+"',"+str(size)+",1,'[\"id\"]','[[\"probe\"]]');")
        result=cli(['--config',directory/'restore/compose.yaml','--env',directory/'restore/.env',
          'audit','--output',directory/'probe-audit.json'],expected=2)
        audit=json.loads((directory/'probe-audit.json').read_text())
        assert {'missingDatasetFile','changedDatasetFile'}.issubset({r['kind'] for r in audit['issues']})
        assert any(r.get('path')=='raw/'+orphan+'.csv' for r in audit['warnings'])
        for id in (missing,changed):assert http(SOURCE,'/datasets/'+id,expected=404)['message']
        cli(['verify',directory/'backup'])
        for item in records:assert capture(SOURCE,item['task']['id'])==item
        for id in dataset_ids:assert http(SOURCE,'/datasets/'+id+'/download',raw=True)==downloads[id]
        assert storage.digest(ROOT/'.env')==original_env
        print('PASS read-only discrepancy detection: additive clone probes report missing, changed and unregistered raw files; backup and original service/data/results remain unchanged',flush=True)
        evidence={'run':str(directory),'backup':saved,'restore':receipt,'historicalSamples':len(history),
          'baselineTask':baseline['id'],'governanceTask':governed['id'],'restoredRerunTask':rerun['id'],
          'inputRows':5,'outputRows':4,'ruleCount':5,'sourceUnchanged':True,
          'probeKinds':sorted({r['kind'] for r in audit['issues']}),'retention':'Clone containers stopped; every volume retained.'}
        storage.save_json(directory/'acceptance.json',evidence)
        print(json.dumps(evidence,ensure_ascii=False),flush=True)
    finally:
        if paused:
            storage.run(source.compose+['up','-d','backend']);healthy(SOURCE)
        if clone:
            storage.run(clone.compose+['stop'],timeout=90)

if __name__=='__main__':main()
