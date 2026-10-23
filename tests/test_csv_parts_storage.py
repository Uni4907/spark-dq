import copy,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import storage as st
class CsvPartsStorageTest(unittest.TestCase):
 def fixture(self):
  node={'version':1,'format':'CSV_PARTS','order':'REQUEST','canonicalSha256':'c'*64,'rowCount':2,'columns':['id'],
    'files':[{'path':f'{n:03d}.csv','name':f'{n}.csv','size':5,'sha256':str(n)*64,'rows':1} for n in (1,2)]}
  metadata={'counts':{},'datasets':[],'tasks':[],'results':[],'snapshotMismatches':[],'missingResults':[],
    'sourceImports':[{'id':'d','sha256':'c'*64,'columns':['id'],'rowCount':2,'manifest':node}]}
  inventory={'files':{f'sources/d/{n:03d}.csv':{'size':5,'sha256':str(n)*64} for n in (1,2)},'bytes':10}
  return metadata,inventory
 def test_valid_source_import_binds_each_part(self):
  m,i=self.fixture();self.assertTrue(st.compare(m,i)['ok'])
  for path in list(i['files']):
   altered=copy.deepcopy(i);altered['files'][path]['sha256']='0'*64;self.assertFalse(st.compare(m,altered)['ok'])
 def test_rejects_metadata_and_path_changes(self):
  for key,value in [('columns',['other']),('rowCount',3),('canonicalSha256','z'*64),('version',2)]:
   m,i=self.fixture();m['sourceImports'][0]['manifest'][key]=value;self.assertFalse(st.compare(m,i)['ok'])
  m,i=self.fixture();m['sourceImports'][0]['manifest']['files'][0]['path']='../escape.csv';self.assertFalse(st.compare(m,i)['ok'])
 def test_missing_source_and_unregistered_failure_diagnostics(self):
  m,i=self.fixture();del i['files']['sources/d/001.csv'];self.assertFalse(st.compare(m,i)['ok'])
  m,i=self.fixture();m['sourceImports']=[];result=st.compare(m,i);self.assertTrue(result['ok']);self.assertEqual('unregisteredSourceDirectory',result['warnings'][0]['kind'])
if __name__=='__main__':unittest.main()
