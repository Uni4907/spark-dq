#!/usr/bin/env python3
"""Real receipt acceptance layered on the isolated Standalone correctness trial."""
import json,subprocess,sys,time
from pathlib import Path
import standalone_trial as t,execution_receipts as e

class ReceiptCluster(t.Cluster):
 def master_url(self):
  cid=next(cid for role,cid in self.containers if role=='master')
  ip=t.st.run(['docker','inspect','--format','{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}',cid]).decode().strip()
  return 'http://'+ip+':8080/json/'
 def receipt(self,name,folder):
  receipt=e.read(folder/'result',folder/'request.json',name)
  t.st.save_json(self.root/(name+'-receipt.json'),receipt);return receipt
 def observation(self,receipt):return e.observe(receipt,e.fetch(self.master_url()))
 def job(self,name,payload,**kwargs):
  result=super().job(name,payload,**kwargs)
  if kwargs.get('async_start') or kwargs.get('main','Engine')!='Engine':return result
  _,folder,code,_=result;receipt=self.receipt(name,folder)
  assert receipt['evidenceState']=='DRIVER_ENDED' and receipt['ended']['stopReturned'] is True
  assert receipt['ended']['driverOutcome']==('COMPLETED' if code==0 else 'FAILED')
  if code==0:
   produced=json.loads((folder/'result/result.json').read_text());assert produced['execution']['receipt']['applicationId']==receipt['started']['applicationId'] and produced['execution']['receipt']['attemptId']==receipt['submitted']['attemptId']
  if kwargs.get('cluster',True):
   end=time.monotonic()+20
   while time.monotonic()<end:
    observation=self.observation(receipt)
    if observation['observation']=='HISTORY':break
    time.sleep(.2)
   assert observation['observation']=='HISTORY',observation
   t.st.save_json(self.root/(name+'-observation.json'),observation)
   # Exercise the actual operator CLI and bounded HTTP fetch, not only direct functions.
   cli=subprocess.run([sys.executable,str(Path(e.__file__)),'--folder',str(folder/'result'),'--request',str(folder/'request.json'),'--task-id',name,'--master-url',self.master_url()],capture_output=True,timeout=15)
   assert cli.returncode==0,cli.stderr.decode();node=json.loads(cli.stdout);assert node['cluster']['observation']=='HISTORY' and node['businessStateChanged'] is False
  return result
 def cancel(self,payload):
  cid,folder=self.job('cancelled',payload,async_start=True);deadline=time.monotonic()+60
  while time.monotonic()<deadline:
   receipt=e.read(folder/'result',folder/'request.json','cancelled')
   if receipt['evidenceState']=='STARTED' and all('CoarseGrainedExecutorBackend' in t.st.run(['docker','top',worker,'-eo','pid,args']).decode() for worker in self.workers):
    observed=self.observation(receipt)
    if observed['observation']=='ACTIVE':break
   if t.st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()!=b'true':raise t.st.StorageError('Crash probe ended before live identity capture')
   time.sleep(.1)
  else:raise t.st.StorageError('Live bound cluster application was not observed')
  t.st.save_json(self.root/'crash-active-observation.json',observed);self.owned(cid)
  t.st.run(['docker','kill','--signal','KILL',cid],timeout=15);self.wait_empty();self.logs(cid,folder/'driver.log')
  receipt=self.receipt('cancelled',folder);assert receipt['evidenceState']=='STARTED' and not (folder/'result/result.json').exists() and not (folder/'result/execution-ended.json').exists()
  deadline=time.monotonic()+30
  while time.monotonic()<deadline:
   observed=self.observation(receipt)
   if observed['observation']=='HISTORY':break
   time.sleep(.2)
  assert observed['observation']=='HISTORY',observed
  t.st.save_json(self.root/'crash-history-observation.json',observed)
  return {'driverContainer':cid,'bothExecutorsAllocated':True,'executorsExited':True,'resultPublished':False,'driverKilled':True,'durableApplicationId':receipt['started']['applicationId'],'missingEndReceiptDoesNotInferRunning':True,'masterHistoryDoesNotInferBusinessSuccess':not observed['businessSuccessInferred']}

def main():
 t.Cluster=ReceiptCluster;t.main()
if __name__=='__main__':main()
