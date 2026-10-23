import test_controlled_storage as controlled
import unittest
class HiveStorageTest(unittest.TestCase):
 def fixture(self):
  m,i=controlled.ControlledStorageTest().fixture();n=m['sourceImports'][0]['manifest'];n['hive']=dict(version=1,catalog='HIVE',database='lab',table='quality',provider='hive',metastore='EMBEDDED_DERBY',sparkVersion='3.5.8',rows=1,columns=['id'],canonicalSha256=n['canonicalSha256'],receiptSha256='a'*64);return m,i
 def test_valid_frozen_catalog_snapshot_survives_without_source_catalog(self):
  from test_storage import storage
  m,i=self.fixture();self.assertTrue(storage.compare(m,i)['ok'])
 def test_changed_catalog_source_receipt_is_rejected(self):
  from test_storage import storage
  for key,value in [('rows',101),('canonicalSha256','0'*64),('provider','parquet'),('table','x;select'),('receiptSha256','bad')]:
   m,i=self.fixture();m['sourceImports'][0]['manifest']['hive'][key]=value;self.assertFalse(storage.compare(m,i)['ok'])
