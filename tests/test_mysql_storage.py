import copy,hashlib,json,tarfile,unittest
from test_storage import storage,archive,metadata

class MysqlStorageTest(unittest.TestCase):
    def fixture(self):
        identity='12345678-1234-1234-1234-123456789abc';raw=b'"id"\r\n"1"\r\n"1"\r\n';digest=hashlib.sha256(raw).hexdigest()
        snapshot={'version':1,'format':'MYSQL_SNAPSHOT','isolation':'REPEATABLE READ; WITH CONSISTENT SNAPSHOT; READ ONLY','schema':'business','table':'sample','mapping':[{'source':'id','target':'id'}],'types':['BIGINT'],'rows':2,'bytes':len(raw),'canonicalSha256':digest}
        encoded=json.dumps(snapshot).encode();manifest_sha=hashlib.sha256(encoded).hexdigest()
        manifest=dict(snapshot,order='UNSPECIFIED',importId=identity,sourceId='fixture',columns=['id'],rowCount=2,snapshotManifestSha256=manifest_sha,files=[{'path':'canonical.csv','name':'mysql-fixture.csv','size':len(raw),'sha256':digest,'rows':2}])
        job={'id':identity,'status':'SUCCEEDED','name':'mysql-fixture.csv','sourceSha256':digest,'sourceBytes':len(raw),'contractVersion':1,'datasetId':identity,'manifestSha256':manifest_sha,'request':{'sourceId':'fixture','columns':['id']}}
        m=metadata();m.update(tasks=[],results=[],datasets=[{'id':identity,'path':'/data/raw/'+identity+'.csv','sha256':digest,'size':len(raw)}],mysqlImports=[job],sourceImports=[{'id':identity,'sha256':digest,'rowCount':2,'columns':['id'],'manifest':manifest}])
        prefix='mysql-imports/'+identity+'/snapshot/';inventory=storage.scan_tar(archive([('raw/'+identity+'.csv',raw,tarfile.REGTYPE),(prefix+'canonical.csv',raw,tarfile.REGTYPE),(prefix+'manifest.json',encoded,tarfile.REGTYPE)]))
        return m,inventory,prefix
    def test_valid_mysql_snapshot_is_auditable(self):
        m,i,_=self.fixture();self.assertTrue(storage.compare(m,i)['ok']);self.assertTrue(i['preparedManifests'])
    def test_corrupt_or_missing_snapshot_fails(self):
        for part in ('canonical.csv','manifest.json'):
            m,i,prefix=self.fixture();i['files'][prefix+part]['sha256']='0'*64;self.assertFalse(storage.compare(m,i)['ok'])
    def test_metadata_mapping_or_job_cannot_drift(self):
        for key,value in [('columns',['other']),('sourceId','different'),('rowCount',3),('snapshotManifestSha256','0'*64)]:
            m,i,_=self.fixture();m['sourceImports'][0]['manifest'][key]=value;self.assertFalse(storage.compare(m,i)['ok'])
    def test_failed_job_cannot_reference_a_dataset(self):
        m,i,_=self.fixture();m['mysqlImports'][0]['status']='FAILED';self.assertFalse(storage.compare(m,i)['ok'])
    def test_failed_unpublished_import_needs_no_source(self):
        m,i,_=self.fixture();m.update(sourceImports=[],datasets=[]);m['mysqlImports'][0].update(status='CANCELLED',datasetId=None,manifestSha256=None,sourceSha256=None,sourceBytes=None);self.assertTrue(storage.compare(m,i)['ok'])

if __name__=='__main__':unittest.main()
