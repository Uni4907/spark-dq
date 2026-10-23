#!/usr/bin/env python3
"""Exercise only a disposable demo API; all expected results use stdlib oracles."""
import argparse,collections,csv,io,json
from pathlib import Path
from iqr_acceptance import Client
from algorithm_oracle import blank,iqr,fd

ROOT=Path(__file__).resolve().parents[1]
def rows(raw):
 return [{k:None if v=='\\N' else v for k,v in row.items()} for row in csv.DictReader(io.StringIO(raw.decode(),newline=''))]
def multiset(values):return collections.Counter(json.dumps(v,ensure_ascii=False,sort_keys=True) for v in values)
def verify(client,task,values,out):
 assert task['status']=='SUCCEEDED' and task['total_rows']==len(values)<50
 for rule in task['rules']:
  kind=rule['type']
  if kind=='IQR':metrics,indices=iqr([r['amount'] for r in values],rule['params']['k']);truth=[values[i] for i in indices]
  elif kind=='FD':metrics,indices=fd(values,['code'],['category']);truth=[values[i] for i in indices]
  elif kind=='MISSING':
   column=[r['note'] for r in values];metrics=dict(nullCount=sum(v is None for v in column),emptyCount=sum(v=='' for v in column),whitespaceCount=sum(v is not None and v!='' and v.strip(' ')=='' for v in column),blankCount=sum(blank(v) for v in column));truth=[r for r in values if blank(r['note'])]
  elif kind=='DUPLICATE':
   groups=collections.Counter(r['id'] for r in values);metrics=dict(duplicateGroups=sum(n>1 for n in groups.values()),duplicateRows=sum(n for n in groups.values() if n>1),excessRows=sum(n-1 for n in groups.values() if n>1));truth=[dict(r,__dq_group_size=groups[r['id']]) for r in values if groups[r['id']]>1]
  else:raise AssertionError(kind)
  assert rule['metrics']==metrics,(kind,rule['metrics'],metrics)
  code,preview=client.call('/tasks/'+task['id']+'/rules/'+rule['id']+'/preview');assert code==200
  assert multiset(preview)==multiset(truth),(kind,preview,truth)
  (out/(task['id']+'-'+kind+'-anomalies.json')).write_text(json.dumps(preview,ensure_ascii=False,indent=2))
 code,report=client.call('/tasks/'+task['id']+'/report');assert code==200 and report['task']['id']==task['id']
 html=client.raw('/tasks/'+task['id']+'/report.html');assert b'<!doctype html>' in html and task['id'].encode() in html
 (out/(task['id']+'.json')).write_text(json.dumps(report,ensure_ascii=False,indent=2));(out/(task['id']+'.html')).write_bytes(html)

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--base',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False);c=Client(a.base)
 raw=(ROOT/'demo/orders.csv').read_bytes();source=rows(raw);assert len(source)==14
 data=c.upload(raw,'demo-orders.csv');request=json.loads((ROOT/'demo/rules.json').read_text());code,template=c.call('/rule-templates',request);assert code==200
 first=c.task(dict(datasetId=data['id'],templateId=template['template_id'],templateRevision=1));verify(c,first,source,a.output)
 policy=json.loads((ROOT/'demo/governance.json').read_text());governed=c.task(policy,'/tasks/'+first['id']+'/govern')
 filled=[dict(r,note='待核实') if blank(r['note']) else dict(r) for r in source];expected=list({r['id']:r for r in filled}.values());assert len(expected)==12
 verify(c,governed,expected,a.output);version=governed['governance']['output_dataset_id'];assert version!=data['id']
 assert multiset(rows(c.raw('/datasets/'+version+'/download')))==multiset(expected)
 replay=c.task({},'/tasks/'+governed['id']+'/rerun');verify(c,replay,expected,a.output)
 assert replay['dataset_id']==version and c.raw('/datasets/'+data['id']+'/download')==raw
 summary=dict(ok=True,datasetId=data['id'],versionId=version,tasks=[first['id'],governed['id'],replay['id']],sourceRows=14,versionRows=12,allFourRulesAndAnomalyMultisetsVerified=True,originalBytesPreserved=True,htmlAndJsonReports=True,base=a.base)
 (a.output/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2));print(json.dumps(summary,ensure_ascii=False))
if __name__=='__main__':main()
