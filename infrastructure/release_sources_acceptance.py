#!/usr/bin/env python3
"""Unified candidate: real MySQL/CSV/Parquet, boundaries, frozen versions and restart."""
from pathlib import Path
import sys,json,csv,io,collections,subprocess,time,uuid,urllib.request,urllib.error,traceback
P=Path('/home/sparkdq/projects/spark-dq');sys.path[:0]=[str(P/'infrastructure'),str(P/'tests')]
import release_lab as lab,mysql_import_trial as trial
from iqr_acceptance import Client,csv_bytes,LOCAL
from algorithm_oracle import blank
root=Path(sys.argv[1]);a=json.loads((root/'access.json').read_text());c=Client(a['apiUrl']);out=root/('sources-acceptance-'+time.strftime('%Y%m%d-%H%M%S'));out.mkdir();summary=dict(ok=False,checks=[])
def get(path):
 code,data=c.call(path);assert code==200,(code,data);return data
def record(name,**v):summary['checks'].append(dict(name=name,**v));lab.save(out/'progress.json',summary);print(name,flush=True)
def rows(raw):return [{k:None if v=='\\N' else v for k,v in row.items()} for row in csv.DictReader(io.StringIO(raw.decode('utf-8'),newline=''))]
def upload_status(raw):
 boundary=uuid.uuid4().hex;body=('--'+boundary+'\r\nContent-Disposition: form-data; name="file"; filename="boundary.csv"\r\n\r\n').encode()+raw+('\r\n--'+boundary+'--\r\n').encode();req=urllib.request.Request(c.base+'/datasets',data=body,headers={'Content-Type':'multipart/form-data; boundary='+boundary})
 try:
  with LOCAL.open(req,timeout=30) as response:return response.status,json.load(response)
 except urllib.error.HTTPError as error:return error.code,json.load(error)
def task_verify(task,values):return trial.validate_task(c,a['backendContainer'],task,values,out)
try:
 count=get('/datasets/page')['total']
 for raw in [b'id,text\r\n1,"'+b'x'*65537+b'"\r\n',b'id\r\n'+b','*100+b'\r\n']:
  status,message=upload_status(raw);assert status==400,(status,message)
 assert get('/datasets/page')['total']==count
 status,message=c.call('/controlled-files/over_rows/register',None,'POST');assert status==400 and get('/datasets/page')['total']==count;(out/'over-row-response.json').write_text(json.dumps(dict(status=status,response=message)));record('early oversized cells/columns and1000001 controlled rows reject without dataset')
 long_rows=[dict(id='1',text='"'*65536),dict(id='1',text='"'*65536)];long_raw=csv_bytes(['id','text'],long_rows);wide=c.upload(long_raw,'maximum-valid-cell.csv');assert all(len(value)<=256 for row in wide['preview'] for value in row);assert c.raw('/datasets/'+wide['id']+'/download')==long_raw
 long_task=c.task(dict(datasetId=wide['id'],rules=[dict(type='DUPLICATE',columns=['id'])]));code,preview=c.call('/tasks/'+long_task['id']+'/rules/'+long_task['rules'][0]['id']+'/preview');assert code==200 and len(preview)==2 and all(len(row['text'])==256 and row['__dq_group_size']==2 for row in preview);full=trial.full_rows(a['backendContainer'],long_task,out);assert all(len(row['text'])==65536 for row in full[long_task['rules'][0]['id']]);lab.save(out/'bounded-preview.json',dict(dataset=wide,task=long_task,anomalyPreview=preview));record('65536 escaped quotes accepted; dataset/anomaly previews bounded; full raw/anomalies intact',taskId=long_task['id'])
 fields=['id','amount','code','category','note'];fixture=json.loads((root/'fixture-rows.json').read_text());job,key=trial.submit(c,'demo',fields);job=trial.wait_import(c,job['id'],'SUCCEEDED');data=get('/datasets/'+job['dataset_id']);raw=c.raw('/datasets/'+data['id']+'/download');assert trial.multiset(rows(raw))==trial.multiset(fixture);assert c.raw('/datasets/'+data['id']+'/sources/1/download')==raw;lab.save(out/'mysql-dataset.json',data);(out/'mysql-snapshot.csv').write_bytes(raw)
 standard=c.upload(csv_bytes(fields,fixture),'matching-MySQL.csv');rules=[dict(type='IQR',columns=['amount']),dict(type='FD',columns=['code'],params=dict(dependentColumns=['category']))];first=c.task(dict(datasetId=data['id'],rules=rules));expected=task_verify(first,fixture);reference=c.task(dict(datasetId=standard['id'],rules=rules));assert task_verify(reference,fixture)==expected;record('unified candidate MySQL and CSV exact full metrics/anomaly multisets',mysqlTaskId=first['id'],csvTaskId=reference['id'],datasetId=data['id'])
 changed=c.task(dict(fills={'note':'待核实'},deduplicate=False),'/tasks/'+first['id']+'/govern');filled=[dict(r,note='待核实') if blank(r['note']) else dict(r) for r in fixture];task_verify(changed,filled);version=get('/datasets/'+changed['governance']['output_dataset_id']);assert trial.multiset(rows(c.raw('/datasets/'+version['id']+'/download')))==trial.multiset(filled);replay=c.task({},'/tasks/'+changed['id']+'/rerun');task_verify(replay,filled);assert replay['dataset_id']==version['id'];record('unified MySQL governance/new version/independent rerun',governTaskId=changed['id'],replayTaskId=replay['id'])
 for alias,error in [('wrong','SNAPSHOT_FAILED'),('row_limit','ROW_LIMIT')]:
  job,_=trial.submit(c,alias,fields);failure=trial.wait_import(c,job['id'],'FAILED');assert failure['error']==error and failure['dataset_id'] is None;lab.save(out/(alias+'-failure.json'),failure)
 assert c.call('/mysql-imports',dict(sourceId='arbitrary',columns=fields),'POST')[0]==400;record('unified white-list, wrong credentials and row bounds reject without snapshot')
 # Native Parquet source fixture from the earlier verified contract, raw bytes fixed.
 source=(P/'tests/fixtures/t10-positive.parquet');boundary=uuid.uuid4().hex;body=('--'+boundary+'\r\nContent-Disposition: form-data; name="file"; filename="regression-positive.parquet"\r\nContent-Type: application/octet-stream\r\n\r\n').encode()+source.read_bytes()+('\r\n--'+boundary+'--\r\n').encode()
 with LOCAL.open(urllib.request.Request(c.base+'/imports',data=body,headers={'Content-Type':'multipart/form-data; boundary='+boundary,'X-Import-Key':str(uuid.uuid4())}),timeout=30) as response:assert response.status==202;native=json.load(response)
 deadline=time.monotonic()+80
 while True:
  native=get('/imports/'+native['id'])
  if native['status'] in ('SUCCEEDED','FAILED','CANCELLED'):break
  assert time.monotonic()<deadline;time.sleep(.2)
 assert native['status']=='SUCCEEDED',native;nd=get('/datasets/'+native['dataset_id']);assert nd['source']['format']=='PARQUET_NATIVE' and c.raw('/datasets/'+nd['id']+'/sources/1/download')==source.read_bytes();canonical=c.raw('/datasets/'+nd['id']+'/download');nr=rows(canonical);col=nd['columns'][0];native_rules=[dict(type='MISSING',columns=[col]),dict(type='DUPLICATE',columns=[col])];nt=c.task(dict(datasetId=nd['id'],rules=native_rules));expected=task_verify(nt,nr)
 # Both execution-copy formats must fit the same immutable snapshot and full results.
 formats=[]
 for fmt in ['CSV','PARQUET']:
  task=c.task(dict(datasetId=nd['id'],rules=native_rules,executionInput=dict(format=fmt,partitions=2)));assert task_verify(task,nr)==expected;assert task['snapshot']['input_manifest_sha256'];formats.append(dict(format=fmt,taskId=task['id']))
 assert c.raw('/datasets/'+nd['id']+'/download')==canonical;record('native Parquet import and CSV/Parquet execution copies preserve full metrics/anomalies',nativeTaskId=nt['id'],datasetId=nd['id'],formats=formats)
 # Simulated backend interruption on a real started Spark; only the owned container is killed.
 code,job=c.call('/tasks',dict(datasetId=data['id'],rules=rules));assert code==200;deadline=time.monotonic()+30
 while True:
  state=get('/tasks/'+job['id'])
  if (state.get('execution') or {}).get('started'):break
  assert time.monotonic()<deadline and state['status'] in ('RUNNING','QUEUED');time.sleep(.2)
 lab.save(out/'before-restart.json',state);subprocess.run(['docker','kill',a['backendContainer']],stdout=subprocess.DEVNULL,check=True,timeout=20);subprocess.run(['docker','start',a['backendContainer']],stdout=subprocess.DEVNULL,check=True,timeout=20);_,_,cmd=lab.context(root);c=Client('http://'+lab.out(cmd+['port','backend','8080'])+'/api');trial.ready(c);a['apiUrl']=c.base;a['frontendUrl']='http://'+lab.out(cmd+['port','frontend','5173']);lab.save(root/'access.json',a);state=get('/tasks/'+job['id']);assert state['status']=='FAILED' and all(r['metrics'] is None for r in state['rules']);lab.save(out/'after-restart.json',state);assert c.raw('/datasets/'+data['id']+'/download')==raw;record('actual Spark interruption/restart marks failure without partial results and preserves MySQL snapshot',taskId=job['id'])
 summary.update(ok=True,candidate=json.loads((root/'lab.json').read_text())['candidate'],apiUrl=c.base)
except Exception:summary['failure']=traceback.format_exc();raise
finally:lab.save(out/'summary.json',summary);print(json.dumps(dict(out=str(out),ok=summary['ok'])),flush=True)
