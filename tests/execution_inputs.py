"""Real HTTP/Spark execution copies; synthetic data only. --check is read-only."""
import argparse,csv,hashlib,io,json,sys,time,uuid
from pathlib import Path
import e2e
def call(path,body,expected=200):
 code,result=e2e.http(path,json.dumps(body,ensure_ascii=False).encode(),{'Content-Type':'application/json'})
 assert code==expected,(path,code,result);return result
def wait(id,status='SUCCEEDED'):
 end=time.monotonic()+320
 while time.monotonic()<end:
  code,task=e2e.http('/tasks/'+id);assert code==200
  if task['status'] in ('SUCCEEDED','FAILED','CANCELLED'):
   assert task['status']==status,task;return task
  time.sleep(.15)
 raise AssertionError('Task deadline '+id)
def download(id):
 with e2e.LOCAL.open(e2e.BASE+'/datasets/'+id+'/download',timeout=30) as response:return response.read()
def metrics(task):return sorted([(r['type'],r['columns'],r['params'],r['metrics']) for r in task['rules']],key=lambda x:(x[0],x[1]))
def previews(task):
 return {r['type']+str(r['columns']):sorted(json.dumps(x,ensure_ascii=False,sort_keys=True) for x in e2e.http('/tasks/'+task['id']+'/rules/'+r['id']+'/preview')[1]) for r in task['rules']}
def check(fixture):
 assert hashlib.sha256(download(fixture['datasetId'])).hexdigest()==fixture['sourceSha256']
 for task in fixture['tasks']:assert e2e.http('/tasks/'+task['id'])[1]==task,task['id']
 assert e2e.http('/tasks/'+fixture['reportTask']+'/report')[1]==fixture['report']
 for id,sha in fixture['outputs'].items():assert hashlib.sha256(download(id)).hexdigest()==sha
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--base',required=True);parser.add_argument('--check',type=Path);args=parser.parse_args();e2e.BASE=args.base
 if args.check:check(json.loads(args.check.read_text()));print('PASS immutable plans/manifests/metrics/versions/reports across restart');return
 columns=['id','name','email','score','day','note']
 rows=[['A','\\N','a@x','9','2026-01-01','中文,\n"one"'],['A','Alice','a@x','10','2026-01-02','winner'],
  ['B','','\\N','2','2026-01-01',''],['C','   ','c@x','3','2026-01-01',' '],
  ['\\N','N','n@x','1','2026-01-01','null-key-low'],['\\N','N2','n2@x','2','2026-01-02','null-key-high']]
 stream=io.StringIO(newline='');writer=csv.writer(stream,quoting=csv.QUOTE_ALL,lineterminator='\r\n');writer.writerow(columns);writer.writerows(rows)
 raw=stream.getvalue().encode();code,dataset=e2e.upload(raw,'execution-input-synthetic.csv');assert code==200
 rules=[{'type':'MISSING','columns':['name']},{'type':'MISSING','columns':['email']},{'type':'DUPLICATE','columns':['id']},
  {'type':'TYPE','columns':['score'],'params':{'dataType':'DECIMAL'}},{'type':'TYPE','columns':['day'],'params':{'dataType':'DATE'}}]
 request={'datasetId':dataset['id'],'rules':rules}
 baseline=wait(call('/tasks',request)['id']);tasks=[baseline]
 count=e2e.http('/tasks/page')[1]['total']
 bad=[{},[],{'format':'ORC','partitions':4},{'format':'CSV','partitions':0},{'format':'CSV','partitions':17},
  {'format':'CSV','partitions':1.5},{'format':'CSV','partitions':'4'},{'format':'CSV_SINGLE','partitions':4},
  {'format':'PARQUET','partitions':4,'path':'/etc/passwd'}]
 for plan in bad:call('/tasks',dict(request,executionInput=plan),400)
 assert e2e.http('/tasks/page')[1]['total']==count
 outputs={};governed=None
 for kind in ('CSV','PARQUET'):
  plan={'format':kind,'partitions':4};task=wait(call('/tasks',dict(request,executionInput=plan))['id']);tasks.append(task)
  snapshot=task['snapshot'];assert snapshot['input_sha256']==dataset['sha256'] and len(snapshot['input_manifest_sha256'])==64
  assert snapshot['executionInput']==dict(version=1,rowCount=6,columns=columns,**plan)
  assert metrics(task)==metrics(baseline) and previews(task)==previews(baseline)
  policy={'deduplicate':True,'fills':{'name':'UNKNOWN','email':'unknown@x'},
   'deduplicationOrder':[{'column':'score','type':'DECIMAL','direction':'DESC','nulls':'LAST'}]}
  governed=wait(call('/tasks/'+task['id']+'/govern',policy)['id']);tasks.append(governed)
  assert governed['snapshot']['executionInput']==snapshot['executionInput']
  assert governed['governance']['input_rows']==6 and governed['total_rows']==4
  output=governed['governance']['output_dataset_id'];body=download(output);outputs[output]=hashlib.sha256(body).hexdigest()
  actual=list(csv.reader(io.StringIO(body.decode(),newline='')));assert actual[0]==columns
  expected=[rows[1],['B','UNKNOWN','unknown@x','2','2026-01-01',''],['C','UNKNOWN','c@x','3','2026-01-01',' '],rows[5]]
  assert sorted(actual[1:])==sorted(expected),(actual,expected)
  rerun=wait(call('/tasks/'+governed['id']+'/rerun',{})['id']);tasks.append(rerun)
  assert rerun['dataset_id']==output and rerun['snapshot']['executionInput']['rowCount']==4
  assert rerun['snapshot']['executionInput']['format']==kind and metrics(rerun)==metrics(governed)
  assert task['snapshot']['input_manifest_sha256']==e2e.http('/tasks/'+task['id'])[1]['snapshot']['input_manifest_sha256']
  print('PASS '+kind+' detection -> frozen plan/manifest -> governance -> canonical version -> inherited-format rerun',flush=True)
 reset=wait(call('/tasks/'+tasks[-1]['id']+'/rerun',{'executionInput':{'format':'CSV_SINGLE','partitions':1}})['id']);tasks.append(reset)
 assert 'executionInput' not in reset['snapshot']
 fixture={'datasetId':dataset['id'],'sourceSha256':hashlib.sha256(raw).hexdigest(),'tasks':tasks,'outputs':outputs,
   'reportTask':governed['id'],'report':e2e.http('/tasks/'+governed['id']+'/report')[1]}
 assert download(dataset['id'])==raw
 root=Path(__file__).resolve().parents[1]/'.runtime';path=root/('execution-fixture-'+uuid.uuid4().hex[:12]+'.json');path.write_text(json.dumps(fixture,ensure_ascii=False,indent=2))
 print(json.dumps({'ok':True,'fixture':str(path),'tasks':len(tasks),'datasetId':dataset['id']}),flush=True)
if __name__=='__main__':main()
