#!/usr/bin/env python3
"""Final-image HTTP regression for strict numeric end anchors and FD coexistence."""
import argparse,json
from pathlib import Path
from iqr_acceptance import Client,csv_bytes,digest
from algorithm_oracle import iqr,fd
def main():
 p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();root=a.output;root.mkdir();c=Client(a.base)
 fixture=json.loads(Path(__file__).with_name('algorithm-independent-cases.json').read_text());raw_values=['1\n',' 1','2 ','1\t',None,'','   ','1.00000000001','NaN','0','1'];large=fixture['iqrCases'][-1]['values']
 rows=[dict(row,numeric=raw_values[i],precision=large[i] if i<len(large) else None) for i,row in enumerate(fixture['fdRows'])]
 raw=csv_bytes(list(rows[0]),rows);(root/'input.csv').write_bytes(raw);data=c.upload(raw,'IQR-FD-final.csv');before=c.raw('/datasets/'+data['id']+'/download');assert before==raw and digest(raw)==data['sha256']
 rules=[{'type':'FD','columns':['x1','x2'],'params':{'dependentColumns':['y1','y2']}},{'type':'IQR','columns':['numeric']},{'type':'IQR','columns':['precision']}]
 first=c.task({'datasetId':data['id'],'rules':rules});replay=c.task({},'/tasks/'+first['id']+'/rerun')
 for t in [first,replay]:
  for rule in t['rules']:
   expected,indexes=fd(rows,rule['columns'],rule['params']['dependentColumns']) if rule['type']=='FD' else iqr([r[rule['columns'][0]] for r in rows],rule['params']['k'])
   assert rule['metrics']==expected,(rule,expected)
   code,actual=c.call(f"/tasks/{t['id']}/rules/{rule['id']}/preview");assert code==200
   assert sorted(json.dumps(row,sort_keys=True) for row in actual)==sorted(json.dumps(rows[i],sort_keys=True) for i in indexes)
  (root/(t['id']+'.json')).write_text(json.dumps(t,ensure_ascii=False,indent=2));report=c.call('/tasks/'+t['id']+'/report')[1];(root/(t['id']+'-report.json')).write_text(json.dumps(report,ensure_ascii=False,indent=2));(root/(t['id']+'-report.html')).write_bytes(c.raw('/tasks/'+t['id']+'/report.html'))
 numeric=next(r for r in first['rules'] if r['columns']==['numeric']);assert numeric['metrics']['checkedCount']==2 and numeric['metrics']['nonNumericCount']==5 and numeric['metrics']['outOfDomainCount']==1 and numeric['metrics']['status']=='INSUFFICIENT_DATA'
 assert c.raw('/datasets/'+data['id']+'/download')==before
 (root/'download-before.csv').write_bytes(before);(root/'download-after.csv').write_bytes(c.raw('/datasets/'+data['id']+'/download'))
 summary={'ok':True,'tasks':[first['id'],replay['id']],'dataset':data['id'],'sourceSha256':digest(raw),'trailingLfTabsSpacesRejected':True,'strictNumericMetrics':numeric['metrics'],'fdGivenCaseAndLargePrecisionMatch':True,'fullAnomalyRowsAndMetricsMatch':True,'originalBytesUnchanged':True};(root/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2));(root/'COMPLETE').write_text(digest((root/'summary.json').read_bytes()));print(json.dumps(summary),flush=True)
if __name__=='__main__':main()
