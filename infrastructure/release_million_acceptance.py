#!/usr/bin/env python3
"""Formal product flow with disk-backed full-multiset verification, no big Python lists."""
from pathlib import Path
import sys,json,time,subprocess,urllib.request,http.client,uuid,hashlib,traceback,csv
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'infrastructure'),str(ROOT/'tests')]
import release_lab as lab,scale_oracle as oracle
from iqr_acceptance import Client,LOCAL
def get(c,path):
 code,value=c.call(path);assert code==200,(code,value);return value
def post(c,path,body=None):
 code,value=c.call(path,body,'POST');assert code==200,(code,value);return value
def download(c,path,target):
 with LOCAL.open(c.base+path,timeout=45) as stream,target.open('xb') as out:
  for block in iter(lambda:stream.read(1024*1024),b''):out.write(block)
 return lab.sha(target)
def verify(c,backend,task,expected,folder):
 folder.mkdir();lab.save(folder/'task.json',task);assert task['status']=='SUCCEEDED'
 subprocess.run(['docker','cp',backend+':/data/tasks/'+task['id'],str(folder/'spark')],check=True,timeout=120)
 result=json.loads((folder/'spark/result.json').read_text());assert result['totalRows']==expected['rows'];checks=[]
 for rule in task['rules']:
  key={'IQR':'iqr','FD':'fd','DUPLICATE':'duplicate'}[rule['type']];assert rule['metrics']==expected['metrics'][key],(key,rule['metrics'],expected['metrics'][key])
  raw=folder/(key+'.actual.unsorted')
  with raw.open('w') as stream:
   for part in (folder/'spark/anomalies'/rule['id']).glob('part-*.json'):
    with part.open() as records:
     for line in records:
      row=json.loads(line);assert set(row)==set(oracle.COLUMNS+(['__dq_group_size'] if key=='duplicate' else []));stream.write(oracle.encode(row))
  actual=oracle.sorted_digest(raw,folder/(key+'.actual.sorted'));assert actual==expected['anomalies'][key],(key,actual,expected['anomalies'][key]);checks.append(dict(ruleId=rule['id'],type=rule['type'],fullMultiset=actual))
 lab.save(folder/'report.json',get(c,'/tasks/'+task['id']+'/report'));download(c,'/tasks/'+task['id']+'/report.html',folder/'report.html');lab.save(folder/'verification.json',dict(ok=True,rows=expected['rows'],rules=checks));return checks
def csv_multiset(path,folder,name):
 raw=folder/(name+'.unsorted')
 with path.open(newline='',encoding='utf-8') as source,raw.open('w') as out:
  for row in csv.DictReader(source):
   assert set(row)==set(oracle.COLUMNS);out.write(oracle.encode({k:None if v=='\\N' else v for k,v in row.items()}))
 return oracle.sorted_digest(raw,folder/(name+'.sorted'))
def reject_upload(c,source,folder):
 from urllib.parse import urlsplit
 url=urlsplit(c.base);conn=http.client.HTTPConnection(url.hostname,url.port,timeout=40);boundary=uuid.uuid4().hex;prefix=('--'+boundary+'\r\nContent-Disposition: form-data; name="file"; filename="must-reject.csv"\r\nContent-Type: text/csv\r\n\r\n').encode();suffix=('\r\n--'+boundary+'--\r\n').encode();size=20*1024**2+1
 try:
  conn.putrequest('POST',url.path+'/datasets');conn.putheader('Content-Type','multipart/form-data; boundary='+boundary);conn.putheader('Content-Length',str(len(prefix)+size+len(suffix)));conn.endheaders();conn.send(prefix)
  try:
   with source.open('rb') as stream:
    remaining=size
    while remaining:
     block=stream.read(min(65536,remaining));conn.send(block);remaining-=len(block)
   conn.send(suffix)
  except (BrokenPipeError,ConnectionResetError):pass
  response=conn.getresponse();body=response.read();receipt=dict(status=response.status,bytes=size,body=body.decode(errors='replace'));lab.save(folder/'ordinary-upload-rejection.json',receipt);assert response.status in (400,413) and any(x in receipt['body'].lower() for x in ['20','size','upload','大小']),receipt
 finally:conn.close()
def main(root):
 root,meta,cmd=lab.context(root);a=json.loads((root/'access.json').read_text());c=Client(a['apiUrl']);folder=root/('million-acceptance-'+time.strftime('%Y%m%d-%H%M%S'));folder.mkdir(mode=0o700);summary=dict(ok=False,checks=[]);source=json.loads((root/'source-oracle/expected.json').read_text());governed=json.loads((root/'governed-oracle/expected.json').read_text())
 def checked(name,**data):summary['checks'].append(dict(name=name,**data));lab.save(folder/'progress.json',summary);print(name,flush=True)
 try:
  views=get(c,'/controlled-files');lab.save(folder/'public-files.json',views);assert len(views)==4 and 'path' not in json.dumps(views);before=get(c,'/datasets/page')['total'];assert c.call('/controlled-files/not_allowed/register',None,'POST')[0]==400;reject_upload(c,root/'inputs/million.csv',folder);checked('ordinary upload still rejects above20MiB; arbitrary alias rejected')
  bad_code,bad=c.call('/controlled-files/bad_hash/register',None,'POST');lab.save(folder/'bad-fingerprint.json',dict(status=bad_code,response=bad));assert bad_code==400 and get(c,'/datasets/page')['total']==before;checked('configured fingerprint mismatch publishes no dataset')
  start=time.monotonic();data=post(c,'/controlled-files/million/register');lab.save(folder/'dataset.json',data);assert data['size_bytes']==source['inputBytes'] and data['row_count']==1000000 and data['size_bytes']>20*1024**2 and data['source']['profile']=='CONTROLLED_64M_V1';same=post(c,'/controlled-files/million/register');assert same['id']==data['id'];assert download(c,'/datasets/'+data['id']+'/sources/1/download',folder/'downloaded-source.csv')==data['sha256'];checked('formal immutable controlled registration and idempotent source reuse',datasetId=data['id'],bytes=data['size_bytes'],registrationSeconds=time.monotonic()-start)
  rules=[dict(type='IQR',columns=['n'],params=dict(k='1.5')),dict(type='FD',columns=['x'],params=dict(dependentColumns=['y'])),dict(type='DUPLICATE',columns=['id'])]
  first=c.task(dict(datasetId=data['id'],rules=rules));verify(c,a['backendContainer'],first,source,folder/'detection');checked('million-row product detection complete full metrics/multisets',taskId=first['id'])
  policy=dict(normalizations={'padding':'UPPER'},deduplicate=True);changed=c.task(policy,'/tasks/'+first['id']+'/govern');verify(c,a['backendContainer'],changed,governed,folder/'governance');version=get(c,'/datasets/'+changed['governance']['output_dataset_id']);assert version['row_count']==500000 and version['size_bytes']>20*1024**2 and version['source']['profile']=='CONTROLLED_64M_V1' and version['source']['parentDatasetId']==data['id'];lab.save(folder/'governed-dataset.json',version);download(c,'/datasets/'+version['id']+'/download',folder/'governed-version.csv');actual=csv_multiset(folder/'governed-version.csv',folder,'governed-full');expected=csv_multiset(root/'governed-oracle/input.csv',folder,'independent-governed-full');assert actual==expected,'Serialized governed multiset differs from independent generated result';lab.save(folder/'governed-full-multiset.json',actual);checked('governed version remains above20MiB and inherits controlled profile',taskId=changed['id'],datasetId=version['id'],rows=500000,bytes=version['size_bytes'])
  replay=c.task({},'/tasks/'+changed['id']+'/rerun');verify(c,a['backendContainer'],replay,governed,folder/'recheck');assert replay['dataset_id']==version['id'];checked('independent-version recheck/report complete',taskId=replay['id'])
  code,job=c.call('/tasks',dict(datasetId=data['id'],rules=rules));assert code==200;deadline=time.monotonic()+40
  while time.monotonic()<deadline:
   task=get(c,'/tasks/'+job['id'])
   if (task.get('execution') or {}).get('started'):break
   assert task['status'] in ('QUEUED','RUNNING');time.sleep(.2)
  else:raise TimeoutError('No actual Spark-start receipt for cancellation')
  lab.save(folder/'cancel-before.json',task);assert c.call('/tasks/'+job['id']+'/cancel',None,'POST')[0]==200;deadline=time.monotonic()+30
  while time.monotonic()<deadline:
   task=get(c,'/tasks/'+job['id'])
   if task['status']=='CANCELLED':break
   time.sleep(.2)
  else:raise TimeoutError('Controlled task did not finish cancellation')
  lab.save(folder/'cancel-after.json',task);assert all(r['metrics'] is None for r in task['rules']);checked('controlled running Spark cancellation releases resources and publishes no results',taskId=job['id'])
  assert download(c,'/datasets/'+data['id']+'/download',folder/'downloaded-after.csv')==data['sha256'];summary.update(ok=True,sourceDatasetId=data['id'],governedDatasetId=version['id'],sourceRows=1000000,governedRows=500000,sourceBytes=data['size_bytes'],governedBytes=version['size_bytes'],engineJarSha256=meta['candidate']['engineJarSha256'],limits=dict(controlledBytes=64*1024**2,controlledRows=1000000,ordinaryUploadBytes=20*1024**2,taskOutputObservedBytes=1024**3,taskTimeoutSeconds=300),scope='Formal product API flow, no performance speedup or500万product claim',oraclePolicy=source['memoryPolicy'])
 except Exception:summary['failure']=traceback.format_exc();(folder/'failure.txt').write_text(summary['failure']);raise
 finally:lab.save(folder/'summary.json',summary);print(json.dumps(dict(folder=str(folder),ok=summary['ok'])),flush=True)
if __name__=='__main__':main(sys.argv[1])
