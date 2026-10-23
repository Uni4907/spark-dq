import copy,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import storage as st
class NativeImportAuditTest(unittest.TestCase):
 def setUp(self):
  self.id='12345678-1234-1234-1234-123456789abc';self.source='a'*64;self.csv='b'*64;self.manifest='c'*64
  self.job={'id':self.id,'name':'sample.parquet','status':'SUCCEEDED','contractVersion':1,'sourceSha256':self.source,'sourceBytes':12,'datasetId':self.id,'manifestSha256':self.manifest}
  self.node={'version':1,'format':'PARQUET_NATIVE','semanticsVersion':'csv-v2','verified':True,'sourceSha256':self.source,'sourceBytes':12,'rowCount':1,'columns':[{'name':'id','type':'bigint','nullable':True}],'conversion':st.NATIVE_CONVERSION,'canonical':{'path':'canonical.csv','size':9,'sha256':self.csv}}
  self.src={'version':1,'format':'PARQUET_NATIVE','order':'SINGLE','importId':self.id,'columns':['id'],'rowCount':1,'canonicalSha256':self.csv,'nativeManifestSha256':self.manifest,'nativeColumns':self.node['columns'],'conversion':st.NATIVE_CONVERSION,'files':[{'path':'source.parquet','name':'sample.parquet','size':12,'sha256':self.source,'rows':1}]}
  self.row={'id':self.id,'sha256':self.csv,'rowCount':1,'columns':['id'],'manifest':self.src}
  self.metadata={'nativeImports':[self.job],'sourceImports':[self.row]};prefix='imports/'+self.id+'/'
  self.inventory={'files':{prefix+'source.parquet':{'size':12,'sha256':self.source},prefix+'prepared/manifest.json':{'size':400,'sha256':self.manifest},prefix+'prepared/canonical.csv':{'size':9,'sha256':self.csv}},'preparedManifests':{prefix+'prepared/manifest.json':self.node}}
 def check(self):
  issues=[];st.compare_native(self.metadata,self.inventory,issues,[]);return issues
 def test_complete_native_binding(self):
  st.validate_native_source(self.row,self.src,['id'],self.metadata);self.assertEqual([],self.check())
 def test_changed_source_rejected(self):
  self.inventory['files']['imports/'+self.id+'/source.parquet']['sha256']='d'*64;self.assertTrue(self.check())
 def test_failed_original_retained_and_partial_not_published(self):
  self.job.update(status='FAILED',datasetId=None,manifestSha256=None);self.assertEqual([],self.check());self.job['datasetId']=self.id;self.assertTrue(self.check())
 def test_changed_manifest_or_canonical_rejected(self):
  self.node['canonical']['sha256']='d'*64;self.assertTrue(self.check())
 def test_source_manifest_cannot_point_to_other_import(self):
  self.src['importId']='other'
  with self.assertRaises(ValueError):st.validate_native_source(self.row,self.src,['id'],self.metadata)
 def test_corrupt_column_descriptor_rejected_cleanly(self):
  self.src['nativeColumns']=[42]
  with self.assertRaises(ValueError):st.validate_native_source(self.row,self.src,['id'],self.metadata)
 def test_native_manifest_captured_from_archive(self):
  import io,json,tarfile
  content=json.dumps(self.node).encode();stream=io.BytesIO()
  with tarfile.open(fileobj=stream,mode='w') as archive:
   info=tarfile.TarInfo('imports/'+self.id+'/prepared/manifest.json');info.size=len(content);archive.addfile(info,io.BytesIO(content))
  stream.seek(0);self.assertEqual(self.node,st.scan_tar(stream)['preparedManifests']['imports/'+self.id+'/prepared/manifest.json'])
 def test_prior_schema_has_no_native_requirement(self):
  issues=[];st.compare_native({}, {'files':{}},issues,[]);self.assertEqual([],issues)
if __name__=='__main__':unittest.main()
