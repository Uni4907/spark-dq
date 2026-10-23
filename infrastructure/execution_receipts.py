#!/usr/bin/env python3
"""Read-only driver receipt / Spark Standalone observation. Never changes task state."""
import argparse,datetime as dt,hashlib,json,re,sys,urllib.request,urllib.parse,uuid
from pathlib import Path

BASE={'version','taskId','attemptId','requestSha256','sparkMaster'}
FILES=('execution-submitted.json','execution-started.json','execution-ended.json')
class ReceiptError(ValueError):pass
def require(value):
 if not value:raise ReceiptError('Invalid or mismatched execution evidence')
def load(path,limit=16384):
 require(path.is_file() and not path.is_symlink())
 with path.open('rb') as stream:raw=stream.read(limit+1)
 require(len(raw)<=limit)
 return json.loads(raw)
def fields(node,extra):require(isinstance(node,dict) and set(node)==BASE|set(extra))
def text(node,key):require(isinstance(node.get(key),str));return node[key]
def timestamp(value):require(isinstance(value,str));dt.datetime.fromisoformat(value.replace('Z','+00:00'))
def read(folder,request,task_id):
 if not any((folder/name).exists() or (folder/name).is_symlink() for name in FILES):return {'evidenceState':'ABSENT'}
 try:
  submitted=load(folder/FILES[0]);fields(submitted,{'submittedAt'})
  require(type(submitted['version']) is int and submitted['version']==1 and submitted['taskId']==task_id and re.fullmatch(r'[A-Za-z0-9_-]{1,128}',task_id))
  require(str(uuid.UUID(text(submitted,'attemptId')))==submitted['attemptId']);require(re.fullmatch(r'[0-9a-f]{64}',text(submitted,'requestSha256')))
  require(len(text(submitted,'sparkMaster'))<=512);timestamp(submitted['submittedAt'])
  require(request.is_file() and not request.is_symlink())
  with request.open('rb') as stream:data=stream.read(1024*1024+1)
  require(len(data)<=1024*1024 and hashlib.sha256(data).hexdigest()==submitted['requestSha256'])
  result={'evidenceState':'SUBMITTED','submitted':submitted}
  if (folder/FILES[1]).exists() or (folder/FILES[1]).is_symlink():
   started=load(folder/FILES[1]);fields(started,{'applicationId','startedAt'});require(all(submitted[key]==started[key] for key in BASE));require(re.fullmatch(r'[A-Za-z0-9_-]{1,128}',text(started,'applicationId')));timestamp(started['startedAt']);result.update(evidenceState='STARTED',started=started)
  if (folder/FILES[2]).exists() or (folder/FILES[2]).is_symlink():
   ended=load(folder/FILES[2]);fields(ended,{'driverOutcome','stopReturned','endedAt'}|({'applicationId'} if 'started' in result else set()));require(all(submitted[key]==ended[key] for key in BASE));timestamp(ended['endedAt']);require(type(ended['stopReturned']) is bool and ended['driverOutcome'] in ('COMPLETED','FAILED'))
   if 'started' in result:require(ended['applicationId']==result['started']['applicationId'])
   else:require(ended['driverOutcome']=='FAILED' and ended['stopReturned'] is False)
   result.update(evidenceState='DRIVER_ENDED',ended=ended)
  return result
 except (OSError,ValueError,TypeError,KeyError):return {'evidenceState':'INVALID','reason':'Receipt missing, damaged or not bound to request'}

def observe(receipt,master):
 """Even FINISHED is only a cluster observation, never proof of business success."""
 if receipt['evidenceState'] in ('ABSENT','INVALID'):return {'observation':'NO_VALID_BINDING'}
 try:
  submitted=receipt['submitted'];require(isinstance(master,dict) and master.get('url')==submitted['sparkMaster'] and master.get('status')=='ALIVE')
  for group in ('activeapps','completedapps'):
   require(isinstance(master.get(group),list) and len(master[group])<=10000)
   require(all(isinstance(app,dict) and all(isinstance(app.get(key),str) and len(app[key])<=256 for key in ('id','name','state')) for app in master[group]))
  if 'started' not in receipt:return {'observation':'APPLICATION_ID_NOT_RECORDED'}
  app_id=receipt['started']['applicationId'];name='SparkDQ-'+submitted['taskId']+'-'+submitted['attemptId']
  matches=[(group,app) for group in ('activeapps','completedapps') for app in master[group] if app['id']==app_id]
  if not matches:return {'observation':'UNKNOWN','reason':'Application absent from this master snapshot; no terminal inference'}
  if len(matches)!=1 or matches[0][1]['name']!=name:return {'observation':'CONFLICT','reason':'Application identity collision or inconsistent master snapshot'}
  group,app=matches[0]
  return {'observation':'ACTIVE' if group=='activeapps' else 'HISTORY','applicationId':app_id,'clusterState':app['state'],'businessSuccessInferred':False,'remoteCleanupConfirmed':False}
 except (ValueError,TypeError,KeyError):return {'observation':'UNAVAILABLE','reason':'Master identity, schema or recovery state not verified'}

class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):raise ReceiptError('Redirect refused')
def fetch(url):
 parsed=urllib.parse.urlsplit(url)
 require(parsed.scheme in ('http','https') and parsed.hostname and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment and parsed.path=='/json/')
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
 with opener.open(url,timeout=5) as response:
  data=response.read(2*1024*1024+1);require(len(data)<=2*1024*1024)
 return json.loads(data)
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--folder',type=Path,required=True);parser.add_argument('--request',type=Path);parser.add_argument('--task-id',required=True)
 group=parser.add_mutually_exclusive_group();group.add_argument('--master-json',type=Path);group.add_argument('--master-url',help='Explicit trusted operator URL ending /json/; never read from task data')
 args=parser.parse_args();receipt=read(args.folder,args.request or args.folder/'request.json',args.task_id);result={'receipt':receipt,'businessStateChanged':False}
 if args.master_json or args.master_url:
  try:master=load(args.master_json,2*1024*1024) if args.master_json else fetch(args.master_url);result['cluster']=observe(receipt,master)
  except (OSError,ValueError,TypeError):result['cluster']={'observation':'UNAVAILABLE','reason':'Master snapshot unavailable'}
 print(json.dumps(result,ensure_ascii=False,indent=2));return 2 if receipt['evidenceState']=='INVALID' or result.get('cluster',{}).get('observation') in ('CONFLICT','UNAVAILABLE') else 0
if __name__=='__main__':sys.exit(main())
