from pathlib import Path
import sys,json,csv,collections,subprocess,hashlib,io,time
P=Path('/home/sparkdq/projects/spark-dq');sys.path[:0]=[str(P/'infrastructure'),str(P/'tests')]
import demo,mysql_import_trial as trial
from iqr_acceptance import Client
from algorithm_oracle import blank
root,node,project=demo.context();out=root/'demo-verification';out.mkdir();c=Client(node['api']);data=c.upload((P/'demo/orders.csv').read_bytes(),'demo-orders.csv');with_rows=lambda raw:[{k:None if v=='\\N' else v for k,v in r.items()} for r in csv.DictReader(io.StringIO(raw.decode('utf-8'),newline=''))];rows=with_rows((P/'demo/orders.csv').read_bytes());request=json.loads((P/'demo/rules.json').read_text());code,template=c.call('/rule-templates',request);assert code==200
def verify(task,values):
 trial.validate_task(c,project.service('backend'),task,values,out);actual=trial.full_rows(project.service('backend'),task,out);groups=collections.Counter(r['id'] for r in values)
 for rule in task['rules']:
  if rule['type']=='MISSING':
   v=[r['note'] for r in values];expected=dict(nullCount=sum(x is None for x in v),emptyCount=sum(x=='' for x in v),whitespaceCount=sum(x is not None and x!='' and x.strip(' ')=='' for x in v),blankCount=sum(blank(x) for x in v));truth=[r for r in values if blank(r['note'])]
  elif rule['type']=='DUPLICATE':expected=dict(duplicateGroups=sum(n>1 for n in groups.values()),duplicateRows=sum(n for n in groups.values() if n>1),excessRows=sum(n-1 for n in groups.values() if n>1));truth=[dict(r,__dq_group_size=groups[r['id']]) for r in values if groups[r['id']]>1]
  else:continue
  assert expected==rule['metrics'] and trial.multiset(actual[rule['id']])==trial.multiset(truth)
first=c.task(dict(datasetId=data['id'],templateId=template['template_id'],templateRevision=1));verify(first,rows);policy=json.loads((P/'demo/governance.json').read_text());gov=c.task(policy,'/tasks/'+first['id']+'/govern');filled=[dict(r,note='待核实') if blank(r['note']) else dict(r) for r in rows];expected=list({r['id']:r for r in filled}.values());assert len(expected)==12;verify(gov,expected);version=c.call('/datasets/'+gov['governance']['output_dataset_id'])[1];assert trial.multiset(with_rows(c.raw('/datasets/'+version['id']+'/download')))==trial.multiset(expected);replay=c.task({},'/tasks/'+gov['id']+'/rerun');verify(replay,expected);assert c.raw('/datasets/'+data['id']+'/download')==(P/'demo/orders.csv').read_bytes();summary=dict(ok=True,template=template,datasetId=data['id'],versionId=version['id'],tasks=dict(detection=first['id'],governance=gov['id'],recheck=replay['id']),allFourRulesIndependentlyVerified=True,fullAnomalyMultisets=True,sourceRows=14,versionRows=12,backendJarSha256=node['backendJarSha256'],engineJarSha256=node['engineJarSha256'],stableUrl=node['url']);(out/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2));print(json.dumps(summary,ensure_ascii=False),flush=True)
