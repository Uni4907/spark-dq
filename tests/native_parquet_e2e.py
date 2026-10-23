"""Synthetic verified native conversion -> existing CSV HTTP API -> Spark/governance/replay."""
import argparse,csv,hashlib,io,json,uuid
from pathlib import Path
import e2e,execution_inputs as x
def check(f):
 assert hashlib.sha256(x.download(f['datasetId'])).hexdigest()==f['canonicalSha256']
 for task in f['tasks']:assert e2e.http('/tasks/'+task['id'])[1]==task
 assert e2e.http('/tasks/'+f['reportTask']+'/report')[1]==f['report']
 assert hashlib.sha256(x.download(f['outputId'])).hexdigest()==f['outputSha256']
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--base',required=True);p.add_argument('--canonical',type=Path);p.add_argument('--expected',type=Path);p.add_argument('--check',type=Path);a=p.parse_args();e2e.BASE=a.base
 if a.check:check(json.loads(a.check.read_text()));print('PASS converted native values/tasks/report/version across restart');return
 assert a.canonical and a.expected;expected=json.loads(a.expected.read_text());assert len(expected['rows'])==6 and len(expected['columns'])==9
 body=a.canonical.read_bytes();code,dataset=e2e.upload(body,'native-parquet-canonical-synthetic.csv');assert code==200 and dataset['row_count']==6 and dataset['columns']==expected['columns']
 # This stage uses the existing CSV registration API. It does not pretend native source provenance is in the DB yet.
 assert 'source' not in dataset
 rules=[{'type':'MISSING','columns':['name']},{'type':'MISSING','columns':['amount']},{'type':'DUPLICATE','columns':['id']},
  {'type':'TYPE','columns':['id'],'params':{'dataType':'INTEGER'}},{'type':'TYPE','columns':['amount'],'params':{'dataType':'DECIMAL'}},{'type':'TYPE','columns':['day'],'params':{'dataType':'DATE'}},{'type':'REGEX','columns':['active'],'params':{'pattern':'true|false'}}]
 request={'datasetId':dataset['id'],'rules':rules};baseline=x.wait(x.call('/tasks',request)['id']);tasks=[baseline]
 by={t['type']+':'+t['columns'][0]:t['metrics'] for t in baseline['rules']}
 assert by['MISSING:name']==dict(nullCount=1,emptyCount=1,whitespaceCount=1,blankCount=3)
 assert by['MISSING:amount']==dict(nullCount=1,emptyCount=0,whitespaceCount=0,blankCount=1)
 assert by['DUPLICATE:id']==dict(duplicateGroups=2,duplicateRows=4,excessRows=2)
 for key,checked in [('TYPE:id',4),('TYPE:amount',5),('TYPE:day',5),('REGEX:active',5)]:assert by[key]==dict(invalidCount=0,checkedCount=checked,skippedBlankCount=6-checked),(key,by)
 prepared=x.wait(x.call('/tasks',dict(request,executionInput={'format':'PARQUET','partitions':4}))['id']);tasks.append(prepared)
 assert x.metrics(prepared)==x.metrics(baseline) and x.previews(prepared)==x.previews(baseline)
 governed=x.wait(x.call('/tasks/'+prepared['id']+'/govern',{'deduplicate':True,'fills':{'name':'UNKNOWN','amount':'0.0000000000','day':'2026-01-01'},'deduplicationOrder':[{'column':'amount','type':'DECIMAL','direction':'DESC','nulls':'LAST'}]})['id']);tasks.append(governed);assert governed['total_rows']==4
 out=governed['governance']['output_dataset_id'];actual=list(csv.reader(io.StringIO(x.download(out).decode(),newline='')))
 expected_rows=[list(expected['rows'][i]) for i in (1,2,3,5)];expected_rows[1][1]='UNKNOWN';expected_rows[2][1]='UNKNOWN'
 assert actual[0]==expected['columns'] and sorted(actual[1:])==sorted(expected_rows),(actual,expected_rows)
 rerun=x.wait(x.call('/tasks/'+governed['id']+'/rerun',{})['id']);tasks.append(rerun);assert x.metrics(rerun)==x.metrics(governed) and rerun['snapshot']['executionInput']['rowCount']==4
 fixture={'datasetId':dataset['id'],'canonicalSha256':hashlib.sha256(body).hexdigest(),'tasks':tasks,'outputId':out,'outputSha256':hashlib.sha256(x.download(out)).hexdigest(),'reportTask':governed['id'],'report':e2e.http('/tasks/'+governed['id']+'/report')[1]}
 path=Path(__file__).resolve().parents[1]/'.runtime'/('native-api-fixture-'+uuid.uuid4().hex[:12]+'.json');path.write_text(json.dumps(fixture,ensure_ascii=False,indent=2));check(fixture)
 print(json.dumps({'ok':True,'fixture':str(path),'tasks':4,'registeredVia':'CSV_API','datasetId':dataset['id']}),flush=True)
if __name__=='__main__':main()
