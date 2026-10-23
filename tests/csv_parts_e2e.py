"""Bounded synthetic multipart registration -> Spark -> governance -> replay. --check is read-only."""
import argparse,csv,hashlib,io,json,time,uuid
from pathlib import Path
import e2e,execution_inputs as x
def upload(parts):
 boundary='parts-'+uuid.uuid4().hex;body=bytearray()
 for name,value in parts:
  body.extend(('--'+boundary+'\r\nContent-Disposition: form-data; name="files"; filename="'+name+'"\r\nContent-Type: text/csv\r\n\r\n').encode());body.extend(value);body.extend(b'\r\n')
 body.extend(('--'+boundary+'--\r\n').encode())
 return e2e.http('/datasets/csv-parts',bytes(body),{'Content-Type':'multipart/form-data; boundary='+boundary})
def raw(rows,bom=False):
 stream=io.StringIO(newline='');w=csv.writer(stream,quoting=csv.QUOTE_ALL,lineterminator='\r\n');w.writerow(['id','name','score','note']);w.writerows(rows)
 return (('\ufeff' if bom else '')+stream.getvalue()).encode()
def source(id,n):
 with e2e.LOCAL.open(e2e.BASE+f'/datasets/{id}/sources/{n}/download',timeout=30) as response:return response.read()
def check(f):
 assert e2e.http('/datasets/'+f['dataset']['id'])[1]==f['dataset']
 assert hashlib.sha256(x.download(f['dataset']['id'])).hexdigest()==f['dataset']['sha256']
 for n,digest in enumerate(f['sourceHashes'],1):assert hashlib.sha256(source(f['dataset']['id'],n)).hexdigest()==digest
 for t in f['tasks']:assert e2e.http('/tasks/'+t['id'])[1]==t
 assert e2e.http('/tasks/'+f['reportTask']+'/report')[1]==f['report']
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--base',required=True);p.add_argument('--check',type=Path);a=p.parse_args();e2e.BASE=a.base
 if a.check:check(json.loads(a.check.read_text()));print('PASS multipart sources/versions/tasks/report survive restart or restore');return
 rows=[['A','\\N','9','中文,\n"quoted"'],['B','','2',''],['C','   ','3',' '],['A','Alice','10','winner'],['\\N','N','1','low'],['\\N','N2','2','high']]
 parts=[('first.csv',raw(rows[:3],True)),('second.csv',raw(rows[3:])),('header-only.csv',raw([]))]
 count=e2e.http('/datasets/page')[1]['total']
 invalid=[parts[:1],[parts[0],parts[0]],[parts[0],('wrong.csv',b'name,id\nx,1\n')],[parts[0],('bad.csv',b'id,name,score,note\n1,x\n')],[parts[0],('invalid.csv',b'\xff\xfe')],[parts[0],('empty.csv',b'')]]
 for items in invalid:
  code,result=upload(items);assert code==400,(code,result)
 assert e2e.http('/datasets/page')[1]['total']==count
 code,data=upload(parts);assert code==200,(code,data);assert data['row_count']==6 and data['columns']==['id','name','score','note']
 assert data['source']['rowCount']==6 and data['source']['canonicalSha256']==data['sha256']
 for n,(name,value) in enumerate(parts,1):
  assert source(data['id'],n)==value
  entry=data['source']['files'][n-1];assert entry['name']==name and entry['sha256']==hashlib.sha256(value).hexdigest()
 for ordinal in (0,4,-1):assert e2e.http(f'/datasets/{data["id"]}/sources/{ordinal}/download')[0]==404
 merged=list(csv.reader(io.StringIO(x.download(data['id']).decode(),newline='')));assert merged[1:]==rows
 code,control=e2e.upload(raw(rows),'csv-parts-control.csv');assert code==200 and 'source' not in control
 rules=[{'type':'MISSING','columns':['name']},{'type':'DUPLICATE','columns':['id']},{'type':'TYPE','columns':['score'],'params':{'dataType':'DECIMAL'}}]
 request={'datasetId':data['id'],'rules':rules};tasks=[x.wait(x.call('/tasks',request)['id'])]
 baseline=x.wait(x.call('/tasks',dict(request,datasetId=control['id']))['id']);tasks.append(baseline)
 assert x.metrics(tasks[0])==x.metrics(baseline) and x.previews(tasks[0])==x.previews(baseline)
 for kind in ('CSV','PARQUET'):
  task=x.wait(x.call('/tasks',dict(request,executionInput={'format':kind,'partitions':4}))['id']);tasks.append(task)
  assert x.metrics(task)==x.metrics(baseline) and x.previews(task)==x.previews(baseline)
 governed=x.wait(x.call('/tasks/'+tasks[-1]['id']+'/govern',{'deduplicate':True,'fills':{'name':'UNKNOWN'},'deduplicationOrder':[{'column':'score','type':'DECIMAL','direction':'DESC','nulls':'LAST'}]})['id']);tasks.append(governed)
 assert governed['total_rows']==4 and governed['snapshot']['executionInput']['format']=='PARQUET'
 out=governed['governance']['output_dataset_id'];actual=list(csv.reader(io.StringIO(x.download(out).decode(),newline='')))
 expected=[rows[3],['B','UNKNOWN','2',''],['C','UNKNOWN','3',' '],rows[5]];assert sorted(actual[1:])==sorted(expected)
 version=e2e.http('/datasets/'+out)[1];assert version['version']['parent_dataset_id']==data['id'] and 'source' not in version
 rerun=x.wait(x.call('/tasks/'+governed['id']+'/rerun',{})['id']);tasks.append(rerun);assert x.metrics(rerun)==x.metrics(governed)
 # Capture after governance adds derivedVersions; this is immutable across restart.
 data=e2e.http('/datasets/'+data['id'])[1]
 fixture={'dataset':data,'sourceHashes':[hashlib.sha256(v).hexdigest() for _,v in parts],'tasks':tasks,'reportTask':governed['id'],'report':e2e.http('/tasks/'+governed['id']+'/report')[1]}
 path=Path(__file__).resolve().parents[1]/'.runtime'/('multipart-fixture-'+uuid.uuid4().hex[:12]+'.json');path.write_text(json.dumps(fixture,ensure_ascii=False,indent=2));check(fixture)
 print(json.dumps({'ok':True,'fixture':str(path),'datasetId':data['id'],'tasks':len(tasks),'rejectedInputs':len(invalid),'sources':len(parts)}),flush=True)
if __name__=='__main__':main()
