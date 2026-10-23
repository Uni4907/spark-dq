#!/usr/bin/env python3
"""Isolated HTTP/Spark IQR contract acceptance, independent Decimal oracle, finite waits."""
import argparse,csv,hashlib,io,json,time,urllib.request,urllib.error,uuid
from pathlib import Path
from algorithm_oracle import iqr
LOCAL=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def digest(b):return hashlib.sha256(b).hexdigest()
class Client:
 def __init__(self,base):self.base=base
 def call(self,path,body=None,method=None):
  data=None if body is None else json.dumps(body,ensure_ascii=False).encode()
  req=urllib.request.Request(self.base+path,data=data,headers={'Content-Type':'application/json'},method=method)
  try:
   with LOCAL.open(req,timeout=30) as res:return res.status,json.load(res)
  except urllib.error.HTTPError as e:return e.code,json.load(e)
 def raw(self,path):
  with LOCAL.open(self.base+path,timeout=30) as res:return res.read()
 def upload(self,raw,name):
  boundary=uuid.uuid4().hex;body=(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\nContent-Type: text/csv\r\n\r\n').encode()+raw+f'\r\n--{boundary}--\r\n'.encode()
  with LOCAL.open(urllib.request.Request(self.base+'/datasets',data=body,headers={'Content-Type':'multipart/form-data; boundary='+boundary}),timeout=30) as res:return json.load(res)
 def task(self,body,path='/tasks'):
  code,t=self.call(path,body);assert code==200,(code,t);deadline=time.monotonic()+320
  while time.monotonic()<deadline:
   code,t=self.call('/tasks/'+t['id']);assert code==200
   if t['status'] in ('SUCCEEDED','FAILED','CANCELLED'):
    assert t['status']=='SUCCEEDED',t;return t
   time.sleep(.25)
  raise TimeoutError(t['id'])
def csv_bytes(columns,rows):
 s=io.StringIO(newline='');w=csv.writer(s,quoting=csv.QUOTE_ALL,lineterminator='\n');w.writerow(columns);w.writerows([['\\N' if row.get(c) is None else row[c] for c in columns] for row in rows]);return s.getvalue().encode()
def verify(c,t,rows,cases,output):
 for rule in t['rules']:
  if rule['type']!='IQR':continue
  name=rule['columns'][0];m,indices=iqr([row[name] for row in rows],rule['params']['k']);assert rule['metrics']==m,(name,rule['metrics'],m)
  code,pred=c.call(f"/tasks/{t['id']}/rules/{rule['id']}/preview");assert code==200
  assert sorted(json.dumps(v,sort_keys=True) for v in pred)==sorted(json.dumps(rows[i],sort_keys=True) for i in indices),(name,pred,indices)
 (output/(t['id']+'.json')).write_text(json.dumps(t,ensure_ascii=False,indent=2))
def main():
 p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--output',type=Path,required=True);args=p.parse_args();root=args.output;root.mkdir();c=Client(args.base)
 cases={'sides':['-100','0','1','2','3','4','5','100'],'fences':['-4.5','0','1','2','3','4','5','9.5'],'zero':['0']*7+['10'],'constant':['7']*8,'precision':[str(9007199254740992+i) for i in range(7)]+['9007199254741092'],'small':['1','2','100'],'none':[None,'','   ','NaN','1e3','1.00000000001','10000000000000000000000000000'],'negative':['-20','-3.1','-2','-1','0','1','2','30'],'decimal':['.0000000001','.0000000002','.0000000003','.0000000004','.0000000005','.0000000006','.0000000007','.0000000100'],'maxprecision':['9999999999999999999999999999.'+s for s in ['0000000001','0000000002','0000000003','0000000004','0000000005','0000000006','0000000007','9999999999']]}
 rows=[dict(row_id='r'+str(i),dupe=str(i//2),date='2026-10-21' if i else 'bad',text='OK' if i else 'bad',**{k:values[i] if i<len(values) else None for k,values in cases.items()}) for i in range(8)]
 raw=csv_bytes(list(rows[0]),rows);(root/'input.csv').write_bytes(raw);data=c.upload(raw,'IQR-contract.csv');original=c.raw('/datasets/'+data['id']+'/download');assert digest(original)==digest(raw)==data['sha256'];(root/'download-before.csv').write_bytes(original)
 definitions=[{'type':'IQR','columns':[name],'params':{}} for name in cases]+[{'type':'MISSING','columns':['small']},{'type':'DUPLICATE','columns':['dupe']},{'type':'TYPE','columns':['date'],'params':{'dataType':'DATE'}},{'type':'RANGE','columns':['sides'],'params':{'min':'0','max':'5'}},{'type':'REGEX','columns':['text'],'params':{'pattern':'OK'}}]
 code,template=c.call('/rule-templates',{'name':'IQR精确边界契约','rules':definitions});assert code==200,template
 first=c.task({'datasetId':data['id'],'templateId':template['template_id'],'templateRevision':1});verify(c,first,rows,cases,root)
 old={r['type']:r['metrics'] for r in first['rules'] if r['type']!='IQR'}
 assert old=={'MISSING':{'nullCount':5,'emptyCount':0,'whitespaceCount':0,'blankCount':5},'DUPLICATE':{'duplicateGroups':4,'duplicateRows':8,'excessRows':4},'TYPE':{'invalidCount':1,'checkedCount':8,'skippedBlankCount':0},'RANGE':{'nonNumericCount':0,'invalidCount':2,'checkedCount':8,'skippedBlankCount':0},'REGEX':{'invalidCount':1,'checkedCount':8,'skippedBlankCount':0}},old
 report=c.call('/tasks/'+first['id']+'/report')[1];snapshot=first['snapshot'];html=c.raw('/tasks/'+first['id']+'/report.html');assert '第一四分位数'.encode() in html and b'9007199254740993.75' in html;(root/'report.html').write_bytes(html)
 changed=json.loads(json.dumps(definitions));changed[0]['params']={'k':'2'}
 assert c.call('/rule-templates/'+template['template_id'],{'name':'IQR修改后','rules':changed,'expectedRevision':1},'PUT')[0]==200
 replay=c.task({},'/tasks/'+first['id']+'/rerun');verify(c,replay,rows,cases,root)
 replay_report=c.call('/tasks/'+replay['id']+'/report')[1];assert len(replay_report['comparison'])==len(definitions)
 assert c.call('/tasks/'+first['id'])[1]['snapshot']==snapshot and c.call('/tasks/'+first['id']+'/report')[1]==report
 rows2=[dict(row,**{name:(str(100+i) if name=='sides' else row[name]) for name in cases}) for i,row in enumerate(rows)]
 data2=c.upload(csv_bytes(list(rows2[0]),rows2),'IQR-new-data.csv');new=c.task({'datasetId':data2['id']},'/tasks/'+first['id']+'/rerun');verify(c,new,rows2,cases,root)
 new_report=c.call('/tasks/'+new['id']+'/report')[1];comparison=next(r for r in new_report['comparison'] if r['type']=='IQR' and r['columns']==['sides']);assert comparison['beforeMetrics']['q1']!=comparison['afterMetrics']['q1']
 assert b'9007199254740993.75' in c.raw('/tasks/'+new['id']+'/report.html')
 download_after=c.raw('/datasets/'+data['id']+'/download');assert download_after==original;(root/'download-after.csv').write_bytes(download_after)
 summary={'downloadBeforeSha256':digest(original),'downloadAfterSha256':digest(download_after),'ok':True,'tasks':[first['id'],replay['id'],new['id']],'allFiveLegacyRulesVerified':True,'exactOracleCases':list(cases),'sourceImmutable':True,'templateRevisionIsolation':True,'refitThresholdsReported':True,'inputSha256':digest(raw),'oracleSha256':digest(Path(__file__).with_name('algorithm_oracle.py').read_bytes()),'generatorSha256':digest(Path(__file__).read_bytes()),'base':args.base,'dataset':data['id']}
 (root/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2));(root/'COMPLETE').write_text(digest((root/'summary.json').read_bytes()));print(json.dumps(summary),flush=True)
if __name__=='__main__':main()
