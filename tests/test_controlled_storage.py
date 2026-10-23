import copy,hashlib,unittest
from test_storage import storage,metadata
class ControlledStorageTest(unittest.TestCase):
 def fixture(self):
  m=metadata();sha=hashlib.sha256(b'x').hexdigest();node=dict(version=1,format='CONTROLLED_CSV',profile='CONTROLLED_64M_V1',kind='PRESET',sourceId='demo',canonicalSha256=sha,rowCount=1,columns=['id'],files=[dict(path='canonical.csv',name='demo.csv',size=1,sha256=sha,rows=1)])
  m['sourceImports']=[dict(id='sample',sha256=sha,rowCount=1,columns=['id'],manifest=node)];inventory=dict(bytes=1,files={'raw/sample.csv':dict(sha256=sha,size=1),'tasks/task/anomalies/rule/part-0.json':dict(sha256=hashlib.sha256(b'').hexdigest(),size=0)});return m,inventory
 def test_controlled_snapshot_is_audited_from_registered_raw_file(self):
  m,i=self.fixture();self.assertTrue(storage.compare(m,i)['ok'])
 def test_changed_profile_hash_size_and_path_are_refused(self):
  for key,value in [('profile','FORGED'),('canonicalSha256','0'*64),('rowCount',1000001)]:
   m,i=self.fixture();m['sourceImports'][0]['manifest'][key]=value;self.assertFalse(storage.compare(m,i)['ok'])
  for key,value in [('path','../escape'),('size',64*1024**2+1),('sha256','0'*64)]:
   m,i=self.fixture();m['sourceImports'][0]['manifest']['files'][0][key]=value;self.assertFalse(storage.compare(m,i)['ok'])
 def test_unknown_or_self_parent_is_not_a_governed_origin(self):
  for parent in ['missing','sample']:
   m,i=self.fixture();m['sourceImports'][0]['manifest'].update(kind='GOVERNED',parentDatasetId=parent);self.assertFalse(storage.compare(m,i)['ok'])
 def test_governed_profile_keeps_existing_parent(self):
  m,i=self.fixture();parent=copy.deepcopy(m['datasets'][0]);parent.update(id='parent',path='/data/raw/parent.csv');m['datasets'].append(parent);i['files']['raw/parent.csv']=dict(i['files']['raw/sample.csv']);m['sourceImports'][0]['manifest'].update(kind='GOVERNED',parentDatasetId='parent');self.assertTrue(storage.compare(m,i)['ok'])
