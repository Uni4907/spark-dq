import hashlib,json,sys,tempfile,unittest,uuid
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import execution_receipts as e
class ReceiptsTest(unittest.TestCase):
 def setup_files(self,root):
  request=root/'request.json';request.write_text('{"taskId":"task"}')
  base={'version':1,'taskId':'task','attemptId':str(uuid.uuid4()),'requestSha256':hashlib.sha256(request.read_bytes()).hexdigest(),'sparkMaster':'spark://master:7077'}
  def write(stage,**extra):(root/('execution-'+stage+'.json')).write_text(json.dumps(dict(base,**extra)))
  write('submitted',submittedAt='2026-10-20T02:00:00Z');write('started',startedAt='2026-10-20T02:00:01Z',applicationId='app-1')
  return request,base,write
 def master(self,receipt,group='activeapps'):
  start=receipt['submitted'];node={'url':start['sparkMaster'],'status':'ALIVE','activeapps':[],'completedapps':[]};node[group]=[{'id':'app-1','name':'SparkDQ-task-'+start['attemptId'],'state':'RUNNING' if group=='activeapps' else 'FINISHED'}];return node
 def test_stage_and_request_binding(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);request,base,write=self.setup_files(root);self.assertEqual(e.read(root,request,'task')['evidenceState'],'STARTED');request.write_text('changed');self.assertEqual(e.read(root,request,'task')['evidenceState'],'INVALID')
 def test_receipt_end_is_not_business_success(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);request,base,write=self.setup_files(root);write('ended',endedAt='2026-10-20T02:00:02Z',applicationId='app-1',driverOutcome='COMPLETED',stopReturned=True);receipt=e.read(root,request,'task');self.assertEqual(receipt['evidenceState'],'DRIVER_ENDED');result=e.observe(receipt,self.master(receipt,'completedapps'));self.assertEqual(result['observation'],'HISTORY');self.assertFalse(result['businessSuccessInferred']);self.assertFalse(result['remoteCleanupConfirmed'])
 def test_missing_master_history_is_unknown(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);request,base,write=self.setup_files(root);receipt=e.read(root,request,'task');master=self.master(receipt);master['activeapps']=[];self.assertEqual(e.observe(receipt,master)['observation'],'UNKNOWN')
 def test_master_restart_id_reuse_and_duplicate_identity_conflict(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);request,base,write=self.setup_files(root);receipt=e.read(root,request,'task');master=self.master(receipt);master['activeapps'][0]['name']='SparkDQ-other';self.assertEqual(e.observe(receipt,master)['observation'],'CONFLICT');master=self.master(receipt);master['completedapps']=master['activeapps'];self.assertEqual(e.observe(receipt,master)['observation'],'CONFLICT')
 def test_foreign_or_unready_master_and_missing_schema_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);request,base,write=self.setup_files(root);receipt=e.read(root,request,'task')
   for key,value in (('url','spark://other:7077'),('status','RECOVERING'),('activeapps',None)):
    master=self.master(receipt);master[key]=value;self.assertEqual(e.observe(receipt,master)['observation'],'UNAVAILABLE')
 def test_unrecorded_application_is_not_guessed_from_task_name(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);request,base,write=self.setup_files(root);(root/'execution-started.json').unlink();receipt=e.read(root,request,'task');self.assertEqual(e.observe(receipt,self.master(receipt))['observation'],'APPLICATION_ID_NOT_RECORDED')
 def test_oversized_symlink_partial_and_unexpected_values_refused(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);request,base,write=self.setup_files(root);path=root/'execution-started.json'
   for content in ('{','x'*16385,json.dumps(dict(base,applicationId='app-1',startedAt='2026-10-20T02:00:01Z',secret='hidden'))):path.write_text(content);result=e.read(root,request,'task');self.assertEqual(result['evidenceState'],'INVALID');self.assertNotIn('hidden',str(result))
   path.unlink();path.symlink_to(request);self.assertEqual(e.read(root,request,'task')['evidenceState'],'INVALID')
 def test_application_end_with_wrong_attempt_or_id_refused(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);request,base,write=self.setup_files(root);write('ended',endedAt='2026-10-20T02:00:02Z',applicationId='app-other',driverOutcome='COMPLETED',stopReturned=True);self.assertEqual(e.read(root,request,'task')['evidenceState'],'INVALID')
 def test_historical_absence_has_no_invented_binding(self):
  with tempfile.TemporaryDirectory() as tmp:self.assertEqual(e.read(Path(tmp),Path(tmp)/'request.json','task'),{'evidenceState':'ABSENT'})
 def test_network_input_requires_explicit_plain_json_endpoint(self):
  for url in ('file:///etc/passwd','http://user:pass@host/json/','http://host/json/?token=secret','http://host/other'):
   with self.assertRaises(e.ReceiptError):e.fetch(url)
if __name__=='__main__':unittest.main()
