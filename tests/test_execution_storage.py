import copy,hashlib,io,json,sys,tarfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import storage as st
class ExecutionStorageTest(unittest.TestCase):
 def fixture(self,status='SUCCEEDED'):
  source=b'id\n1\n';data=b'part';fingerprint=lambda x:hashlib.sha256(x).hexdigest()
  node={'version':1,'format':'CSV','semanticsVersion':'csv-v2','sourceSha256':fingerprint(source),'columns':['id'],'rowCount':1,
    'files':[{'path':'data/part-0.csv','size':len(data),'sha256':fingerprint(data)}]}
  raw=json.dumps(node).encode();stream=io.BytesIO()
  with tarfile.open(fileobj=stream,mode='w') as archive:
   for name,value in [('raw/d.csv',source),('tasks/t/prepared/manifest.json',raw),('tasks/t/prepared/data/part-0.csv',data)]:
    info=tarfile.TarInfo(name);info.size=len(value);archive.addfile(info,io.BytesIO(value))
  stream.seek(0);inventory=st.scan_tar(stream)
  metadata={'counts':{},'datasets':[{'id':'d','path':'/data/raw/d.csv','size':len(source),'sha256':fingerprint(source)}],
   'tasks':[{'id':'t','status':status}],'results':[],'snapshotMismatches':[],'missingResults':[],
   'preparedInputs':[{'taskId':'t','status':status,'sourceSha256':fingerprint(source),'manifestSha256':fingerprint(raw),
     'plan':{'version':1,'format':'CSV','partitions':4,'columns':['id'],'rowCount':1}}]}
  return metadata,inventory
 def test_scan_and_audit_bind_prepared_artifacts(self):
  m,i=self.fixture();self.assertTrue(st.compare(m,i)['ok']);self.assertEqual(i['preparedManifests']['tasks/t/prepared/manifest.json']['rowCount'],1)
 def test_missing_modified_manifest_and_part_detected(self):
  for path in ('tasks/t/prepared/manifest.json','tasks/t/prepared/data/part-0.csv'):
   m,i=self.fixture();i['files'][path]['sha256']='0'*64;self.assertFalse(st.compare(m,i)['ok'])
   del i['files'][path];self.assertFalse(st.compare(m,i)['ok'])
 def test_false_row_count_path_and_duplicate_part_rejected(self):
  for kind in ('row','path','duplicate'):
   m,i=self.fixture();node=i['preparedManifests']['tasks/t/prepared/manifest.json']
   if kind=='row':node['rowCount']=2
   if kind=='path':node['files'][0]['path']='../escape'
   if kind=='duplicate':node['files'].append(copy.deepcopy(node['files'][0]))
   self.assertFalse(st.compare(m,i)['ok'])
 def test_failed_partial_allowed_successful_missing_receipt_rejected(self):
  m,i=self.fixture('CANCELLED');m['preparedInputs'][0]['manifestSha256']=None;self.assertTrue(st.compare(m,i)['ok'])
  m['preparedInputs'][0]['status']='SUCCEEDED';self.assertFalse(st.compare(m,i)['ok'])
 def test_bound_artifact_on_cancelled_task_still_checked(self):
  m,i=self.fixture('CANCELLED');del i['files']['tasks/t/prepared/data/part-0.csv'];self.assertFalse(st.compare(m,i)['ok'])
if __name__=='__main__':unittest.main()
