import copy,hashlib,json,tempfile,unittest,sys,csv,io
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import native_parquet as n,storage as st,prepared_inputs as p
class NativeParquetTest(unittest.TestCase):
 def fixture(self,root):
  (root/'inputs').mkdir();(root/'inputs/source.parquet').write_bytes(b'PAR1xxxxPAR1');(root/'worker/imported').mkdir(parents=True)
  canonical=root/'worker/imported/canonical.csv';canonical.write_text('"id","name"\r\n"1",""\r\n',newline='')
  st.save_json(root/'source.json',dict(st.digest(root/'inputs/source.parquet'),originalName='x.parquet'))
  node={'version':1,'format':'PARQUET_NATIVE','semanticsVersion':'csv-v2','verified':True,'sourceSha256':st.digest(root/'inputs/source.parquet')['sha256'],'sourceBytes':12,'rowCount':1,
   'columns':[{'name':'id','type':'bigint','nullable':True},{'name':'name','type':'string','nullable':True}],
   'canonical':dict(st.digest(canonical),path='canonical.csv'),'conversion':{'version':1,'timezone':'UTC','decimal':'PLAIN_WITH_SCALE','integer':'BASE10','boolean':'LOWERCASE','date':'GREGORIAN_1582_10_15_TO_9999','string':'STRICT_UTF8','order':'UNSPECIFIED'}}
  st.save_json(root/'worker/imported/manifest.json',node);proof=n.verify(root,False);st.save_json(root/'COMPLETE',proof);return node
 def test_readonly_verifier_binds_source_canonical_and_completion(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);self.fixture(root);self.assertEqual(1,n.verify(root)['rows'])
   (root/'inputs/source.parquet').write_bytes(b'PAR1zzzzPAR1');self.assertRaises(st.StorageError,n.verify,root)
 def test_changed_csv_even_with_resigned_manifest_must_match_columns_rows(self):
  for content in ['"other","name"\n"1",""\n','"id","name"\n','"id","name"\n"1","","extra"\n']:
   with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp);node=self.fixture(root);csv_path=root/'worker/imported/canonical.csv';csv_path.write_text(content)
    node['canonical']=dict(st.digest(csv_path),path='canonical.csv');(root/'worker/imported/manifest.json').write_text(json.dumps(node));self.assertRaises(st.StorageError,n.verify,root,False)
 def test_unknown_type_version_contract_and_traversal_rejected(self):
  for kind in ('type','version','contract','path','corrupt'):
   with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp);node=self.fixture(root)
    if kind=='type':node['columns'][0]['type']='timestamp'
    if kind=='version':node['version']=2
    if kind=='contract':node['conversion']['timezone']='SYSTEM'
    if kind=='path':node['canonical']['path']='../other.csv'
    (root/'worker/imported/manifest.json').write_text('[]' if kind=='corrupt' else json.dumps(node));self.assertRaises(st.StorageError,n.verify,root,False)
 def test_symlink_and_missing_completion_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);self.fixture(root);(root/'COMPLETE').unlink();self.assertRaises(st.StorageError,n.verify,root)
   canonical=root/'worker/imported/canonical.csv';canonical.rename(root/'outside');canonical.symlink_to(root/'outside');self.assertRaises(st.StorageError,n.verify,root,False)
 def test_wrong_source_pin_rejected_before_output_or_docker_mutation(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);source=root/'data.parquet';source.write_bytes(b'PAR1xxxxPAR1')
   with patch.object(st,'ROOT',root),patch.object(st,'run') as run,patch.object(n.shutil,'disk_usage',return_value=type('Disk',(),{'free':10*1024**3})()):
    (root/'.runtime').mkdir();output=root/'.runtime/new'
    self.assertRaises(st.StorageError,n.prepare,source,output,'0'*64,'image',{'known':True,'freeBytes':10*1024**3});run.assert_not_called();self.assertFalse(output.exists())
 def test_worker_rejects_conflicting_operation_before_creating_folder(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);folder=root/'work'
   self.assertRaises(st.StorageError,p.worker,folder,root,{},'image','session',prepare=True,import_parquet=True);self.assertFalse(folder.exists())
if __name__=='__main__':unittest.main()
