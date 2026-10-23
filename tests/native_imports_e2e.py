"""Real native Parquet upload, idempotency, durable failure and downstream Spark checks."""
import argparse,csv,io,json,time,uuid,hashlib
from pathlib import Path
import e2e,execution_inputs as x

def upload(data,name,key=None):
 boundary=uuid.uuid4().hex
 body=(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\nContent-Type: application/octet-stream\r\n\r\n').encode()+data+f'\r\n--{boundary}--\r\n'.encode()
 headers={'Content-Type':'multipart/form-data; boundary='+boundary}
 if key is not None:headers['X-Import-Key']=key
 return e2e.http('/imports',body,headers)

def wait(identity):
 end=time.monotonic()+330
 while time.monotonic()<end:
  code,row=e2e.http('/imports/'+identity);assert code==200,(code,row)
  if row['status'] in ('SUCCEEDED','FAILED','CANCELLED'):return row
  time.sleep(.5)
 raise AssertionError('Native import deadline exceeded')

def download(path):
 with e2e.LOCAL.open(e2e.BASE+path,timeout=30) as response:return response.read()

def check(f):
 assert e2e.http('/imports/'+f['import']['id'])[1]==f['import']
 assert e2e.http('/datasets/'+f['datasetId'])[1]['source']==f['source']
 assert hashlib.sha256(download('/datasets/'+f['datasetId']+'/sources/1/download')).hexdigest()==f['import']['source_sha256']
 for task in f['tasks']:assert e2e.http('/tasks/'+task['id'])[1]==task
 assert e2e.http('/tasks/'+f['reportTask']+'/report')[1]==f['report']
 assert hashlib.sha256(x.download(f['outputId'])).hexdigest()==f['outputSha256']

def run(source,expected,failures=None):
 body=source.read_bytes();oracle=json.loads(expected.read_text());key=str(uuid.uuid4())
 before=e2e.http('/imports/page')[1]['total'];code,job=upload(body,'typed-native-synthetic.parquet',key);assert code==202,(code,job)
 assert upload(body,'typed-native-synthetic.parquet',key)[1]['id']==job['id']
 assert upload(body,'different.parquet',key)[0]==409
 assert upload(body+b'x','typed-native-synthetic.parquet',key)[0]==409
 assert upload(body,'typed-native-synthetic.parquet')[0]==400
 assert e2e.http('/imports/page?pageSize=101')[0]==400
 assert e2e.http('/imports/page?page=-1')[0]==400
 job=wait(job['id']);assert job['status']=='SUCCEEDED',job
 assert upload(body,'typed-native-synthetic.parquet',key)[1]==job
 assert e2e.http('/imports/page')[1]['total']==before+1
 dataset=e2e.http('/datasets/'+job['dataset_id'])[1];assert dataset['id']==job['id'] and dataset['row_count']==6 and dataset['columns']==oracle['columns']
 assert download('/datasets/'+dataset['id']+'/sources/1/download')==body
 assert e2e.http('/datasets/'+dataset['id']+'/sources/2/download')[0]==404
 assert e2e.http('/imports/'+job['id']+'/cancel',b'',method='POST')[0]==409
 actual=list(csv.reader(io.StringIO(x.download(dataset['id']).decode(),newline='')))
 assert actual[0]==oracle['columns'] and sorted(actual[1:])==sorted(oracle['rows']),(actual,oracle)
 rules=[{'type':'MISSING','columns':['name']},{'type':'MISSING','columns':['amount']},{'type':'DUPLICATE','columns':['id']},
 {'type':'TYPE','columns':['id'],'params':{'dataType':'INTEGER'}},{'type':'TYPE','columns':['amount'],'params':{'dataType':'DECIMAL'}},{'type':'TYPE','columns':['day'],'params':{'dataType':'DATE'}},{'type':'REGEX','columns':['active'],'params':{'pattern':'true|false'}}]
 request={'datasetId':dataset['id'],'rules':rules};baseline=x.wait(x.call('/tasks',request)['id']);tasks=[baseline]
 by={t['type']+':'+t['columns'][0]:t['metrics'] for t in baseline['rules']}
 assert by['MISSING:name']==dict(nullCount=1,emptyCount=1,whitespaceCount=1,blankCount=3)
 assert by['MISSING:amount']==dict(nullCount=1,emptyCount=0,whitespaceCount=0,blankCount=1)
 assert by['DUPLICATE:id']==dict(duplicateGroups=2,duplicateRows=4,excessRows=2)
 for field,count in [('TYPE:id',4),('TYPE:amount',5),('TYPE:day',5),('REGEX:active',5)]:assert by[field]==dict(invalidCount=0,checkedCount=count,skippedBlankCount=6-count)
 prepared=x.wait(x.call('/tasks',dict(request,executionInput={'format':'PARQUET','partitions':4}))['id']);tasks.append(prepared);assert x.metrics(prepared)==x.metrics(baseline) and x.previews(prepared)==x.previews(baseline)
 governed=x.wait(x.call('/tasks/'+prepared['id']+'/govern',{'deduplicate':True,'fills':{'name':'UNKNOWN','amount':'0.0000000000','day':'2026-01-01'},'deduplicationOrder':[{'column':'amount','type':'DECIMAL','direction':'DESC','nulls':'LAST'}]})['id']);tasks.append(governed);assert governed['total_rows']==4
 output=governed['governance']['output_dataset_id'];actual=list(csv.reader(io.StringIO(x.download(output).decode(),newline='')))
 wanted=[list(oracle['rows'][i]) for i in (1,2,3,5)];wanted[1][1]='UNKNOWN';wanted[2][1]='UNKNOWN';assert actual[0]==oracle['columns'] and sorted(actual[1:])==sorted(wanted),(actual,wanted)
 replay=x.wait(x.call('/tasks/'+governed['id']+'/rerun',{})['id']);tasks.append(replay);assert x.metrics(replay)==x.metrics(governed)
 rejected=[]
 if failures:
  for name in ('float','crlf','expanded'):
   payload=(failures/(name+'.parquet')).read_bytes();failure_key=str(uuid.uuid4());code,failed=upload(payload,name+'.parquet',failure_key);assert code==202,(code,failed);failed=wait(failed['id']);assert failed['status']=='FAILED' and failed['dataset_id'] is None and failed['manifest_sha256'] is None,failed
   assert e2e.http('/datasets/'+failed['id'])[0]==404
   assert upload(payload,name+'.parquet',failure_key)[1]==failed;rejected.append(failed)
  retry_key=str(uuid.uuid4());failed=rejected[0];path='/imports/'+failed['id']+'/retry';code,retry=e2e.http(path,b'',{'X-Import-Key':retry_key},'POST');assert code==202,(code,retry)
  assert e2e.http(path,b'',{'X-Import-Key':retry_key},'POST')[1]['id']==retry['id'];retry=wait(retry['id']);assert retry['status']=='FAILED' and retry['parent_import_id']==failed['id'] and retry['source_sha256']==failed['source_sha256'];rejected.append(retry)
 fixture={'import':job,'datasetId':dataset['id'],'source':dataset['source'],'tasks':tasks,'outputId':output,'outputSha256':hashlib.sha256(x.download(output)).hexdigest(),'reportTask':governed['id'],'report':e2e.http('/tasks/'+governed['id']+'/report')[1],'rejected':rejected}
 path=Path(__file__).resolve().parents[1]/'.runtime'/('imports-api-fixture-'+uuid.uuid4().hex[:12]+'.json');path.write_text(json.dumps(fixture,ensure_ascii=False,indent=2));check(fixture);return {'ok':True,'fixture':str(path),'tasks':4,'failedImports':len(rejected),'importId':job['id']}

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--base',required=True);p.add_argument('--source',type=Path);p.add_argument('--expected',type=Path);p.add_argument('--failures',type=Path);p.add_argument('--check',type=Path);a=p.parse_args();e2e.BASE=a.base
 if a.check:check(json.loads(a.check.read_text()));print('PASS native source, tasks, report and governed CSV persisted')
 else:print(json.dumps(run(a.source,a.expected,a.failures)),flush=True)
