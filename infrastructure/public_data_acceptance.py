#!/usr/bin/env python3
"""Public data correctness, injection-target effect and independent governance verification."""
from pathlib import Path
import csv,json,sys,time,collections,subprocess,traceback,resource,hashlib
P=Path('/home/sparkdq/projects/spark-dq');sys.path[:0]=[str(P/'infrastructure'),str(P/'tests')]
import t9_public_lab as lab
from iqr_acceptance import Client
from algorithm_oracle import iqr,fd,blank
root=Path(sys.argv[1]);data=Path(json.loads((root/'inputs.json').read_text())['publicDataDirectory']);access=json.loads((root/'access.json').read_text());client=Client(access['apiUrl']);out=root/('acceptance-'+time.strftime('%Y%m%d-%H%M%S'));out.mkdir(mode=0o700);summary=dict(ok=False,stages={})
def load(path):
 with path.open(encoding='utf-8',newline='') as f:return [{k:None if v=='\\N' else v for k,v in row.items()} for row in csv.DictReader(f)]
def encode(r):return json.dumps(r,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def multiset(rows):return collections.Counter(map(encode,rows))
def get(path):
 code,value=client.call(path);assert code==200,(code,value);return value
def oracle(rows,kind):
 if kind=='IQR':return iqr([r['age'] for r in rows],'1.5')
 if kind=='FD':return fd(rows,['month'],['dq_month_season'])
 if kind=='MISSING':
  values=[r['age'] for r in rows];n=sum(v is None for v in values);e=sum(v=='' for v in values);w=sum(v is not None and v!='' and v.strip(' ')=='' for v in values)
  return dict(nullCount=n,emptyCount=e,whitespaceCount=w,blankCount=n+e+w),[i for i,v in enumerate(values) if blank(v)]
 groups=collections.defaultdict(list)
 for i,row in enumerate(rows):groups[row['row_id']].append(i)
 duplicates=[g for g in groups.values() if len(g)>1];idx=[i for g in duplicates for i in g]
 return dict(duplicateGroups=len(duplicates),duplicateRows=len(idx),excessRows=sum(len(g)-1 for g in duplicates)),idx
def verify(task,rows,name):
 folder=out/name;folder.mkdir();assert task['status']=='SUCCEEDED';lab.save(folder/'task.json',task);subprocess.run(['docker','cp',access['backendContainer']+':/data/tasks/'+task['id'],str(folder/'spark')],check=True,timeout=90);result=json.loads((folder/'spark/result.json').read_text());assert result['totalRows']==len(rows);results={};predictions={}
 group_sizes=collections.Counter(r['row_id'] for r in rows)
 for rule in task['rules']:
  kind=rule['type'];expected,indices=oracle(rows,kind);assert rule['metrics']==expected,(kind,rule['metrics'],expected)
  truth=multiset([dict(rows[i],__dq_group_size=group_sizes[rows[i]['row_id']]) if kind=='DUPLICATE' else rows[i] for i in indices]);actual=collections.Counter();ids=collections.Counter();count=0;digest=hashlib.sha256()
  for part in sorted((folder/'spark/anomalies'/rule['id']).glob('part-*.json')):
   with part.open() as stream:
    for line in stream:
     row=json.loads(line);actual[encode(row)]+=1;ids[row['row_id']]+=1;count+=1
  assert actual==truth,(kind,sum((actual-truth).values()),sum((truth-actual).values()))
  with (folder/(kind+'-complete-multiset.jsonl')).open('x') as stream:
   for record,multiplicity in sorted(actual.items()):
    line=json.dumps(dict(row=json.loads(record),multiplicity=multiplicity),ensure_ascii=False,sort_keys=True)+'\n';stream.write(line);digest.update(line.encode())
  results[kind]=dict(metrics=expected,fullAnomalyRows=count,completeMultisetSha256=digest.hexdigest());predictions[kind]=ids
 lab.save(folder/'verification.json',dict(ok=True,rows=len(rows),rules=results));lab.save(folder/'report.json',get('/tasks/'+task['id']+'/report'));(folder/'report.html').write_bytes(client.raw('/tasks/'+task['id']+'/report.html'));return results,predictions
def scores(predicted,labels):
 tp=sum((predicted&labels).values());fp=sum((predicted-labels).values());fn=sum((labels-predicted).values());precision=tp/(tp+fp) if tp+fp else None;recall=tp/(tp+fn) if tp+fn else None;f1=2*tp/(2*tp+fp+fn) if 2*tp+fp+fn else None
 return dict(tp=tp,fp=fp,fn=fn,precision=precision,recall=recall,f1=f1,predictedRows=sum(predicted.values()),labelRows=sum(labels.values()))
try:
 baseline=load(data/'baseline.csv');injected=load(data/'injected.csv');expected_governed=load(data/'governed-reference.csv');changes=json.loads((data/'injections.json').read_text());assert len(baseline)==45211 and len(injected)==45231
 rules=[dict(type='MISSING',columns=['age']),dict(type='DUPLICATE',columns=['row_id']),dict(type='IQR',columns=['age'],params=dict(k='1.5')),dict(type='FD',columns=['month'],params=dict(dependentColumns=['dq_month_season']))]
 ds0=client.upload((data/'baseline.csv').read_bytes(),'UCI-Bank-baseline.csv');ds1=client.upload((data/'injected.csv').read_bytes(),'UCI-Bank-controlled-injections.csv');lab.save(out/'baseline-dataset.json',ds0);lab.save(out/'injected-dataset.json',ds1)
 task0=client.task(dict(datasetId=ds0['id'],rules=rules));r0,p0=verify(task0,baseline,'baseline');summary['stages']['baseline']=dict(taskId=task0['id'],rows=len(baseline),rules=r0);print('baseline all metrics and full anomaly multisets verified',flush=True)
 task1=client.task(dict(datasetId=ds1['id'],rules=rules));r1,p1=verify(task1,injected,'injected');summary['stages']['injected']=dict(taskId=task1['id'],rows=len(injected),rules=r1);print('injected all metrics and full anomaly multisets verified',flush=True)
 effect={}
 for kind in ['MISSING','IQR','FD']:
  labels=collections.Counter(v['row_id'] for v in changes if v['kind']==kind);effect[kind+'_modified_row_targets']=scores(p1[kind],labels)
 for kind in ['FD','DUPLICATE']:
  _,idx=oracle(injected,kind);labels=collections.Counter(injected[i]['row_id'] for i in idx);effect[kind+'_rule_participant_targets']=scores(p1[kind],labels)
 effect['DUPLICATE_inserted_row_targets']=scores(p1['DUPLICATE'],collections.Counter(v['row_id'] for v in changes if v['kind']=='DUPLICATE_INSERTION'))
 effect['definitions']='Unit is row occurrence (Counter per stable row_id), exact multiplicity. For copies, insertion labels count1 versus participant labels count2. FP means non-injected according to controlled labels; NOT established real business errors. FD participant truth is independently computed configured rule truth, not an error-source label.'
 lab.save(out/'effect.json',effect)
 gov=client.task(dict(fills={'age':'40'},deduplicate=True),'/tasks/'+task1['id']+'/govern');rg,pg=verify(gov,expected_governed,'governance');version=get('/datasets/'+gov['governance']['output_dataset_id']);assert version['version']['parent_dataset_id']==ds1['id'] and version['row_count']==45211;lab.save(out/'governed-dataset.json',version);(out/'governed-product.csv').write_bytes(client.raw('/datasets/'+version['id']+'/download'));assert multiset(load(out/'governed-product.csv'))==multiset(expected_governed);summary['stages']['governance']=dict(taskId=gov['id'],datasetId=version['id'],rows=len(expected_governed),rules=rg);print('governed full version and all anomalies verified',flush=True)
 replay=client.task({},'/tasks/'+gov['id']+'/rerun');rr,pr=verify(replay,expected_governed,'recheck');assert rr==rg and replay['dataset_id']==version['id'];summary['stages']['recheck']=dict(taskId=replay['id'],rows=len(expected_governed),rules=rr)
 assert client.raw('/datasets/'+ds1['id']+'/download')==(data/'injected.csv').read_bytes();assert get('/tasks/'+task1['id'])['snapshot']==task1['snapshot'];summary.update(ok=True,effect=effect,candidate=json.loads((root/'lab.json').read_text())['candidate'],publicDataDirectory=str(data),allCompleteMetricsAndMultisets=True,originalSnapshotUnchanged=True,peakPythonResidentKiB=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,scope='Public real covariates with explicit artificial quality injections; functional local product experiment, not a performance comparison')
except Exception:summary['failure']=traceback.format_exc();raise
finally:lab.save(out/'summary.json',summary);print(json.dumps(dict(out=str(out),ok=summary['ok'])),flush=True)
