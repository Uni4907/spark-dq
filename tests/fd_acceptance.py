#!/usr/bin/env python3
"""Configured FD acceptance. Tuple/set oracle is independent from Spark grouping/join."""
import argparse,json
from pathlib import Path
from iqr_acceptance import Client,csv_bytes,digest
from algorithm_oracle import fd,iqr
def verify(c,t,rows,root):
 for rule in t['rules']:
  m,indices=fd(rows,rule['columns'],rule['params']['dependentColumns']) if rule['type']=='FD' else iqr([row[rule['columns'][0]] for row in rows],rule['params']['k'])
  assert rule['metrics']==m,(rule, m)
  code,pred=c.call(f"/tasks/{t['id']}/rules/{rule['id']}/preview");assert code==200
  assert sorted(json.dumps(v,sort_keys=True) for v in pred)==sorted(json.dumps(rows[i],sort_keys=True) for i in indices),(rule,pred,indices)
 (root/(t['id']+'.json')).write_text(json.dumps(t,ensure_ascii=False,indent=2))
def main():
 p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();root=a.output;root.mkdir();c=Client(a.base)
 fixture=Path(__file__).with_name('algorithm-independent-cases.json');independent=json.loads(fixture.read_text());rows=[dict(row,num=str(i),select='K',Case='A',case='B') for i,row in enumerate(independent['fdRows'])]
 columns=list(rows[0]);raw=csv_bytes(columns,rows);(root/'input.csv').write_bytes(raw);data=c.upload(raw,'FD-composite.csv');before=c.raw('/datasets/'+data['id']+'/download');assert before==raw and digest(before)==data['sha256'];(root/'download-before.csv').write_bytes(before)
 def rule(x,y):return {'type':'FD','columns':x,'params':{'dependentColumns':y}}
 rules=[rule(['x1','x2'],['y1','y2']),rule(['x1'],['y1']),rule(['x1'],['y1','y2']),rule(['id'],['y1']),rule(['x1','x2'],['y2']),rule(['select','Case'],['case']),{'type':'IQR','columns':['num'],'params':{}}]
 code,template=c.call('/rule-templates',{'name':'FD组合元组与多数派反例','rules':rules});assert code==200
 first=c.task({'datasetId':data['id'],'templateId':template['template_id'],'templateRevision':1});verify(c,first,rows,root)
 primary=next(r for r in first['rules'] if r['columns']==['x1','x2'] and r['params']=={'dependentColumns':['y1','y2']});assert {k:primary['metrics'][k] for k in ('checkedCount','skippedCount','checkedGroups','conflictGroups','conflictRows')}=={'checkedCount':9,'skippedCount':2,'checkedGroups':5,'conflictGroups':2,'conflictRows':5}
 predicted=c.call(f"/tasks/{first['id']}/rules/{primary['id']}/preview")[1];assert sorted(row['id'] for row in predicted)==independent['fdExpected']['anomalyIds']
 old_report=c.call('/tasks/'+first['id']+'/report')[1];html=c.raw('/tasks/'+first['id']+'/report.html');assert '函数依赖冲突'.encode() in html and '冲突涉及行数'.encode() in html;(root/'report.html').write_bytes(html)
 update=json.loads(json.dumps(rules));update[0]['params']['dependentColumns']=['y1'];assert c.call('/rule-templates/'+template['template_id'],{'name':'修改FD依赖项','rules':update,'expectedRevision':1},'PUT')[0]==200
 replay=c.task({},'/tasks/'+first['id']+'/rerun');verify(c,replay,rows,root);report=c.call('/tasks/'+replay['id']+'/report')[1];assert len(report['comparison'])==len(rules)
 extra=[dict(id='e'+str(i),x1=x,x2='1',y1=y,y2='x',num='0',select='select',Case='A',case='B') for i,(x,y) in enumerate([('A','多'),(' A','少'),('a','另'),('中文','一'),('中文','二'),('\t','x'),('', '空'),('   ','空'),('E',''),('E',None),('E','  ')])]
 edge=rows+extra;raw2=csv_bytes(columns,edge);(root/'edge.csv').write_bytes(raw2);data2=c.upload(raw2,'FD-text-blank.csv');new=c.task({'datasetId':data2['id']},'/tasks/'+first['id']+'/rerun');verify(c,new,edge,root)
 blank_rows=[dict((key,('blank'+str(i) if key=='id' else None)) for key in columns) for i in range(3)]
 blank_data=c.upload(csv_bytes(columns,blank_rows),'FD-all-skipped.csv');blank_task=c.task({'datasetId':blank_data['id']},'/tasks/'+first['id']+'/rerun');verify(c,blank_task,blank_rows,root)
 assert all(v['metrics']['checkedCount']==0 and v['metrics']['status']==('NO_VALID_ROWS' if v['type']=='FD' else 'INSUFFICIENT_DATA') for v in blank_task['rules'])
 assert c.call('/tasks/'+first['id']+'/report')[1]==old_report and c.call('/tasks/'+first['id'])[1]['snapshot']==first['snapshot']
 after=c.raw('/datasets/'+data['id']+'/download');assert after==before;(root/'download-after.csv').write_bytes(after)
 for bad in [rule(['x1'],['x1']),rule(['x1'],[]),rule(['x1'],['notHere']),rule(['x1'],['y1','y1'])]:assert c.call('/tasks',{'datasetId':data['id'],'rules':[bad]})[0]==400
 summary={'ok':True,'allSkippedTask':blank_task['id'],'tasks':[first['id'],replay['id'],new['id'],blank_task['id']],'dataset':data['id'],'template':template['template_id'],'independentGivenCaseSha256':digest(fixture.read_bytes()),'firstPrimaryMetrics':primary['metrics'],'firstPrimaryAnomalyIds':sorted(row['id'] for row in predicted),'allMetricAndFullRowMultisetsMatch':True,'templateRevisionIsolation':True,'sourceImmutable':True,'downloadBeforeSha256':digest(before),'downloadAfterSha256':digest(after),'inputSha256':digest(raw),'generatorSha256':digest(Path(__file__).read_bytes()),'oracleSha256':digest(Path(__file__).with_name('algorithm_oracle.py').read_bytes()),'base':a.base}
 (root/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2));(root/'COMPLETE').write_text(digest((root/'summary.json').read_bytes()));print(json.dumps(summary),flush=True)
if __name__=='__main__':main()
