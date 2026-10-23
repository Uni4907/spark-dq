#!/usr/bin/env python3
"""Real Spark ordered retention. Creates only new synthetic datasets/tasks/versions."""
import argparse,copy,csv,hashlib,io,json,uuid,collections
from pathlib import Path
import e2e
ROOT=Path(__file__).resolve().parent.parent
def call(path,body,expected=200):
 code,result=e2e.http(path,json.dumps(body,ensure_ascii=False).encode(),{'Content-Type':'application/json'})
 assert code==expected,(path,code,result)
 return result
def done(created,success=True):
 task=e2e.wait(created['id']);assert task['status']==('SUCCEEDED' if success else 'FAILED'),task
 return task
def download(id):
 with e2e.LOCAL.open(e2e.BASE+'/datasets/'+id+'/download',timeout=30) as response:return response.read()
def rows(task):
 id=task['governance']['output_dataset_id'];raw=download(id)
 dataset=e2e.http('/datasets/'+id)[1]
 assert dataset['sha256']==hashlib.sha256(raw).hexdigest() and dataset['row_count']==task['total_rows']
 assert dataset['version']['governance_task_id']==task['id']
 return list(csv.reader(io.StringIO(raw.decode('utf-8-sig'),newline='')))[1:]
def order(column,type='TEXT',direction='DESC',nulls='LAST'):
 return {'column':column,'type':type,'direction':direction,'nulls':nulls}
def metrics(task):
 return sorted([(x['type'],x['columns'],x['metrics']) for x in task['rules']],key=lambda x:(x[0],x[1]))
def check(fixture):
 assert download(fixture['dataset']['id'])==bytes.fromhex(fixture['rawHex'])
 for task in fixture['tasks']:
  assert e2e.http('/tasks/'+task['id'])[1]==task
  if (task.get('governance') or {}).get('output_dataset_id'):
   assert hashlib.sha256(download(task['governance']['output_dataset_id'])).hexdigest()==fixture['digests'][task['id']]
 assert e2e.http('/tasks/'+fixture['reportTask']+'/report')[1]==fixture['report']
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--base',default='http://127.0.0.1:18080/api');p.add_argument('--check',type=Path);args=p.parse_args();e2e.BASE=args.base
 if args.check:check(json.loads(args.check.read_text()));print('PASS immutable priority policy/audit/versions/results/report persistence');return
 # "count" is deliberately a valid business-key name, not an internal aggregate alias.
 columns=['count','score','day','label','note']
 source=[
  ['A','9','2026-01-03','z','first'],['A','10','2026-01-01','b','middle'],['A','10','2026-01-02','a','latest'],
  ['B','\\N','2026-01-01','n','null'],['B','','2026-01-01','e','empty'],['B','   ','2026-01-01','s','spaces'],['B','2','2026-01-01','v','value'],
  ['C','9007199254740992.0000000001','2026-01-01','a','small'],['C','9007199254740992.0000000002','2026-01-01','b','large'],
  ['D','4','2026-01-01','b','tie-b'],['D','4','2026-01-01','a','中文,"quote"\nC:\\folder'],
  ['\\N','3','2026-01-01','a','null-key-low'],['\\N','4','2026-01-01','b','null-key-high']]
 stream=io.StringIO(newline='');writer=csv.writer(stream,quoting=csv.QUOTE_ALL,lineterminator='\r\n');writer.writerow(columns);writer.writerows(source);raw=stream.getvalue().encode()
 code,dataset=e2e.upload(raw,'retention-priority-synthetic.csv');assert code==200
 rules=[{'type':'DUPLICATE','columns':['count']},{'type':'MISSING','columns':['score']},{'type':'TYPE','columns':['score'],'params':{'dataType':'DECIMAL'}}]
 parent=done(call('/tasks',{'datasetId':dataset['id'],'rules':rules}));tasks=[parent]
 total=e2e.http('/tasks/page')[1]['total']
 bad=[{'deduplicate':False,'deduplicationOrder':[order('score')]},
  {'deduplicate':True,'deduplicationOrder':[order('unknown')]},
  {'deduplicate':True,'deduplicationOrder':[order('score'),order('score')]},
  {'deduplicate':True,'deduplicationOrder':[order('score')]*6},
  {'deduplicate':True,'deduplicationOrder':[None]},
  {'deduplicate':True,'deduplicationOrder':[order('score',type='INTEGER')]},
  {'deduplicate':True,'deduplicationOrder':[order('score',direction='desc')]},
  {'deduplicate':True,'deduplicationOrder':[order('score',nulls='AUTO')]},
  {'deduplicate':True,'deduplicationOrder':[dict(order('score'),column=1)]},
  {'deduplicate':True,'deduplicationOrder':[dict(order('score'),extra=True)]},
  {'deduplicate':True,'deduplicationOrder':[{'column':'score'}]},
  {'deduplicate':True,'deduplicationOrder':[order('score')],'policyVersion':3},
  {'deduplicate':True,'deduplicationOrder':[order('score')],'policyVersion':2},
  {'deduplicate':True,'deduplicationOrder':[order('score')],'policyVersion':5}]
 for policy in bad:call('/tasks/'+parent['id']+'/govern',policy,400)
 assert e2e.http('/tasks/page')[1]['total']==total,'Invalid priorities queued work'
 print('PASS validation: strict shape/enums, existing unique fields, five-level limit, v4 and deduplication required; no invalid task queued',flush=True)
 def govern(policy):
  task=done(call('/tasks/'+parent['id']+'/govern',policy));tasks.append(task);return task
 def by(task):return {x[0]:x for x in rows(task)}
 def clean(task):assert all(r['metrics']['duplicateRows']==0 for r in task['rules'] if r['type']=='DUPLICATE')
 base={'deduplicate':True}
 legacy=govern(base);assert legacy['governance']['policy']=={'policyVersion':2,'fills':{},'normalizations':{},'dropMissing':False,'deduplicate':True}
 assert 'deduplication' not in legacy['governance']['audit']
 lexical=govern(dict(base,deduplicationOrder=[order('score')]))
 assert by(lexical)['A'][1]=='9'
 primary=[order('score','DECIMAL'),order('day','DATE')]
 typed=govern(dict(base,deduplicationOrder=primary))
 actual=by(typed);assert typed['total_rows']==5
 assert actual['A'][1:4]==['10','2026-01-02','a'] and actual['B'][1]=='2'
 assert actual['C'][1]=='9007199254740992.0000000002' and actual['D'][3]=='a' and actual['\\N'][1]=='4'
 assert '中文' in actual['D'][4] and '\n' in actual['D'][4]
 clean(typed);audit=typed['governance']['audit']
 assert audit['policyVersion']==4 and audit['deduplicatedRows']==8
 assert audit['deduplication']=={'order':primary,'tieBreak':'FULL_ROW_LEXICOGRAPHIC_ASC_NULLS_FIRST','inputRows':13,'duplicateGroups':5}
 primary[0]['direction']='ASC';assert e2e.http('/tasks/'+typed['id'])[1]==typed
 ascending=govern(dict(base,deduplicationOrder=[order('score','DECIMAL','ASC','LAST')]))
 assert by(ascending)['A'][1]=='9' and by(ascending)['C'][1]=='9007199254740992.0000000001'
 first=govern(dict(base,deduplicationOrder=[order('score','DECIMAL','DESC','FIRST')]))
 assert by(first)['B'][1]=='\\N'
 asc_first=govern(dict(base,deduplicationOrder=[order('score','DECIMAL','ASC','FIRST')]))
 assert by(asc_first)['B'][1]=='\\N'
 date_first=govern(dict(base,deduplicationOrder=[order('day','DATE'),order('score','DECIMAL','ASC')]))
 assert by(date_first)['A'][1:3]==['9','2026-01-03']
 assert by(typed)['A'][1:3]!=by(date_first)['A'][1:3]
 composite_parent=done(call('/tasks',{'datasetId':dataset['id'],'rules':[{'type':'DUPLICATE','columns':['count','day']}]}));tasks.append(composite_parent)
 composite=done(call('/tasks/'+composite_parent['id']+'/govern',dict(base,deduplicationOrder=[order('score','DECIMAL')])));tasks.append(composite)
 composite_rows=rows(composite)
 assert len(composite_rows)==7 and len({(x[0],x[2]) for x in composite_rows})==7
 assert {(x[1],x[2]) for x in composite_rows if x[0]=='A'}=={('9','2026-01-03'),('10','2026-01-01'),('10','2026-01-02')}
 assert next(x for x in composite_rows if x[0]=='B')[1]=='2'
 assert next(x for x in composite_rows if x[0]=='\\N')[1]=='4'
 assert composite['governance']['audit']['deduplication']['duplicateGroups']==4
 clean(composite)
 print('PASS Spark numeric vs lexical ordering, 10-digit decimal precision, dates, compound order, ASC/DESC with FIRST/LAST, NULL/composite keys, meaningful priority precedence and stable full-row ties',flush=True)
 combined=govern(dict(base,policyVersion=4,normalizations={'count':'LOWER'},mappings={'score':[{'from':'9','to':'20'}]},
  fills={'score':'12'},dropMissing=True,deduplicationOrder=[order('score','DECIMAL')]))
 actual=by(combined);assert actual['a'][1]=='20' and actual['b'][1]=='12'
 assert combined['governance']['audit']['deduplication']['inputRows']==13 and combined['governance']['audit']['droppedMissingRows']==0
 dropped=govern(dict(base,dropMissing=True,deduplicationOrder=[order('score','DECIMAL')]))
 assert dropped['governance']['audit']['deduplication']['inputRows']==10 and dropped['governance']['audit']['droppedMissingRows']==3
 second=done(call('/tasks/'+typed['id']+'/govern',{'deduplicate':True,'deduplicationOrder':[order('score','DECIMAL')]}));tasks.append(second)
 assert collections.Counter(map(tuple,rows(second)))==collections.Counter(map(tuple,rows(typed))) and second['governance']['audit']['deduplicatedRows']==0
 assert second['governance']['audit']['deduplication']['duplicateGroups']==0
 rerun=done(call('/tasks/'+typed['id']+'/rerun',{}));tasks.append(rerun);assert metrics(rerun)==metrics(typed)
 print('PASS transforms/map/fill/drop before retention, v4 with v3 mapping, independent versions, idempotence, real-file recheck and rerun',flush=True)
 # Invalid typed data must fail without a governed version/results; source stays immutable.
 bad_raw=b'count,score,day,label,note\nX,bad,2026-02-30,a,original\nX,2,2026-01-01,b,valid\n'
 code,bad_dataset=e2e.upload(bad_raw,'retention-invalid-synthetic.csv');assert code==200
 bad_parent=done(call('/tasks',{'datasetId':bad_dataset['id'],'rules':[{'type':'DUPLICATE','columns':['count']}]}));tasks.append(bad_parent)
 for field,type in [('score','DECIMAL'),('day','DATE')]:
  failed=done(call('/tasks/'+bad_parent['id']+'/govern',dict(base,deduplicationOrder=[order(field,type)])),False);tasks.append(failed)
  assert field in failed['error'] and type in failed['error']
  assert failed['governance']['output_dataset_id'] is None and failed['governance']['audit'] is None
  assert all(x['result_id'] is None for x in failed['rules'])
 assert download(bad_dataset['id'])==bad_raw and e2e.http('/tasks/'+bad_parent['id'])[1]==bad_parent
 repaired=done(call('/tasks/'+bad_parent['id']+'/govern',dict(base,mappings={'score':[{'from':'bad','to':'3'}]},deduplicationOrder=[order('score','DECIMAL')])));tasks.append(repaired)
 assert rows(repaired)[0][1]=='3'
 code,zero_dataset=e2e.upload(b'count,score,day,label,note\nX,,2026-01-01,a,drop\nX,,2026-01-02,b,drop\n','retention-empty-synthetic.csv');assert code==200
 zero_parent=done(call('/tasks',{'datasetId':zero_dataset['id'],'rules':[{'type':'DUPLICATE','columns':['count']},{'type':'MISSING','columns':['score']}]}));tasks.append(zero_parent)
 zero=done(call('/tasks/'+zero_parent['id']+'/govern',dict(base,dropMissing=True,deduplicationOrder=[order('score','DECIMAL')])));tasks.append(zero)
 assert zero['total_rows']==0 and rows(zero)==[] and zero['governance']['audit']['deduplication']['duplicateGroups']==0
 assert zero['governance']['audit']['deduplication']['inputRows']==0
 # No duplicate rule must still reject before enqueue.
 code,missing_dataset=e2e.upload(b'id,name\n1,A\n','retention-no-key-synthetic.csv');assert code==200
 missing=done(call('/tasks',{'datasetId':missing_dataset['id'],'rules':[{'type':'MISSING','columns':['name']}]}));tasks.append(missing)
 call('/tasks/'+missing['id']+'/govern',dict(base,deduplicationOrder=[order('name')]),400)
 print('PASS malformed numeric/date fail safely, mapping can repair before ranking, missing-key rule rejected and empty output retained',flush=True)
 report=e2e.http('/tasks/'+typed['id']+'/report')[1]
 assert report['task']['governance']==typed['governance'] and len(report['comparison'])==3
 assert download(dataset['id'])==raw and e2e.http('/tasks/'+parent['id'])[1]==parent
 fixture={'dataset':dataset,'rawHex':raw.hex(),'tasks':tasks,'digests':{t['id']:hashlib.sha256(download(t['governance']['output_dataset_id'])).hexdigest() for t in tasks if (t.get('governance') or {}).get('output_dataset_id')},'reportTask':typed['id'],'report':report}
 path=ROOT/'.runtime'/('retention-'+uuid.uuid4().hex[:12]+'.json');path.write_text(json.dumps(fixture,ensure_ascii=False,indent=2))
 check(fixture);print(json.dumps({'ok':True,'fixture':str(path),'sourceTask':parent['id'],'typedTask':typed['id'],'combinedTask':combined['id'],'zeroTask':zero['id'],'rerunTask':rerun['id'],'tasksChecked':len(tasks),'audit':typed['governance']['audit']},ensure_ascii=False),flush=True)
if __name__=='__main__':main()
