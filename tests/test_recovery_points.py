"""Recovery-point privacy, consistency and legacy-catalog regression checks."""
import copy,hashlib,json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import storage as st,backup_catalog as bc
from test_operations import backup

def project(rows=None,native=True,column_type='varchar(36)'):
 names=sorted(set(st.TABLES)|({'dataset_imports'} if native else set()))
 schema=[dict(table=name,name='id',type=column_type,nullable='NO',collation='utf8mb4_0900_ai_ci') for name in names]
 response=''.join('SPARKDQ_POINT_'+name+'\n'+''.join(row+'\n' for row in sorted((rows or {}).get(name,[]))) for name in names)
 p=Mock();p.rows.return_value=schema;p.mysql.side_effect=[('\n'.join(names+['flyway_schema_history'])+'\n').encode(),response.encode(),('\n'.join(names+['flyway_schema_history'])+'\n').encode()];return p

def point(rows=None,native=True):return st.recovery_point(project(rows,native))

def recorded(root,p):
 folder=backup(root);manifest=json.loads((folder/'manifest.json').read_text())
 manifest['counts']={name:record['rows'] for name,record in p['tables'].items()};manifest['counts']['flyway_schema_history']=5
 manifest['recoveryPoint']=p
 (folder/'manifest.json').write_text(json.dumps(manifest));(folder/'COMPLETE').write_text(json.dumps(st.digest(folder/'manifest.json')))
 return folder

class RecoveryPointTest(unittest.TestCase):
 def test_same_records_have_same_root_across_capture_times(self):
  a=point({'tasks':['a'*64]});b=point({'tasks':['a'*64]});self.assertEqual(a['sha256'],b['sha256']);self.assertEqual(a['tables']['tasks']['rows'],1)
 def test_concurrent_schema_change_is_refused(self):
  p=project();before=copy.deepcopy(p.rows.return_value);after=copy.deepcopy(before);after[0]['type']='text';p.rows.side_effect=[before,after]
  with self.assertRaises(st.StorageError):st.recovery_point(p)
 def test_equal_counts_with_changed_content_do_not_match(self):
  a=point({'tasks':['a'*64]});b=point({'tasks':['b'*64]});self.assertNotEqual(a['sha256'],b['sha256']);self.assertEqual(a['tables']['tasks']['rows'],b['tables']['tasks']['rows'])
 def test_duplicate_row_digests_are_counted(self):
  a=point({'tasks':['a'*64,'a'*64]});self.assertEqual(a['tables']['tasks']['rows'],2);self.assertNotEqual(a['sha256'],point({'tasks':['a'*64]})['sha256'])
 def test_schema_change_is_detected(self):
  self.assertNotEqual(point()['sha256'],st.recovery_point(project(column_type='text'))['sha256'])
 def test_v4_and_v5_layouts_valid(self):
  st.validate_recovery_point(point(native=False));st.validate_recovery_point(point())
  self.assertTrue(st.supported_tables(set(st.TABLES)|{'flyway_schema_history','dataset_imports'}));self.assertFalse(st.supported_tables(set(st.TABLES)|{'dataset_imports'}))
 def test_transaction_is_read_only_bounded_and_values_stay_in_mysql(self):
  p=project();st.recovery_point(p);sql=p.mysql.call_args_list[1].args[0]
  for required in ('READ ONLY','WITH CONSISTENT SNAPSHOT',"time_zone='+00:00'",'MAX_EXECUTION_TIME=5000','SHA2(CAST(JSON_ARRAY(','ORDER BY 1 LIMIT 100001'):self.assertIn(required,sql)
  for forbidden in ('INSERT ','UPDATE ','DELETE ','CREATE ','DROP '):self.assertNotIn(forbidden,sql)
 def test_bad_column_identifier_never_reaches_digest_query(self):
  p=project();p.rows.return_value[0]['name']='id` FROM secrets --'
  with self.assertRaises(st.StorageError):st.recovery_point(p)
  self.assertEqual(p.mysql.call_count,1)
 def test_record_budget_refuses_partial_fingerprint(self):
  with patch.object(st,'MAX_POINT_ROWS',1),self.assertRaises(st.StorageError):point({'tasks':['a'*64,'b'*64]})
 def test_missing_or_invalid_response_rejected(self):
  p=project();p.mysql.side_effect=[('\n'.join(st.TABLES)+'\n').encode(),b'SPARKDQ_POINT_tasks\nnot-a-digest\n']
  with self.assertRaises(st.StorageError):st.recovery_point(p)
 def test_changed_root_and_count_binding_rejected(self):
  p=point();p['tables']['tasks']['rows']=1
  with self.assertRaises(st.StorageError):st.validate_recovery_point(p)
  p['sha256']=st.point_digest(p['tables']);counts={t:r['rows'] for t,r in p['tables'].items()};counts.update(tasks=0,flyway_schema_history=5)
  with self.assertRaises(st.StorageError):st.validate_recovery_point(p,counts)
 def test_boolean_count_is_not_integer(self):
  p=point();p['tables']['tasks']['rows']=True;p['sha256']=st.point_digest(p['tables'])
  with self.assertRaises(st.StorageError):st.validate_recovery_point(p)

class CatalogCoverageTest(unittest.TestCase):
 def report(self,root):
  with patch.object(st,'verify',return_value={}):return bc.catalog(root,verify_latest=1)
 def test_v5_catalog_accepts_bound_point_and_matches(self):
  p=point()
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);recorded(root,p);report=self.report(root)
   with patch.object(st,'recovery_point',return_value=p):result=bc.business_coverage(report,Mock())
   self.assertEqual(result['status'],'matches');self.assertTrue(result['matchingBackup']);self.assertEqual(report['entries'][0]['recoveryPoint'],'recorded')
 def test_same_counts_changes_are_listed(self):
  before=point({'tasks':['a'*64]});after=point({'tasks':['b'*64]})
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);recorded(root,before);report=self.report(root)
   with patch.object(st,'recovery_point',return_value=after):result=bc.business_coverage(report,Mock())
   self.assertEqual(result['status'],'changed');self.assertEqual(result['changedTables'],[{'table':'tasks','backupRows':1,'currentRows':1,'reason':'record-or-schema-fingerprint'}])
 def test_legacy_without_point_is_unknown_not_false_coverage(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);backup(root);report=self.report(root)
   with patch.object(st,'recovery_point',return_value=point()):result=bc.business_coverage(report,Mock())
   self.assertEqual(result['status'],'unknown');self.assertIsNone(result['matchingBackup'])
 def test_unverified_backup_cannot_establish_coverage(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);recorded(root,point());report=bc.catalog(root)
   with patch.object(st,'recovery_point',return_value=point()):result=bc.business_coverage(report,Mock())
   self.assertEqual(result['status'],'unknown')
 def test_changed_metadata_after_verification_cannot_reuse_it(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);folder=recorded(root,point());report=self.report(root);manifest=json.loads((folder/'manifest.json').read_text());manifest['recoveryPoint']=point({'tasks':['b'*64]});manifest['counts']['tasks']=1
   (folder/'manifest.json').write_text(json.dumps(manifest));(folder/'COMPLETE').write_text(json.dumps(st.digest(folder/'manifest.json')))
   with patch.object(st,'recovery_point',return_value=manifest['recoveryPoint']):result=bc.business_coverage(report,Mock())
   self.assertEqual(result['status'],'unknown');self.assertEqual(report['entries'][0]['businessCoverage'],'unavailable')
 def test_corrupt_metadata_after_verification_is_reported(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);folder=recorded(root,point());report=self.report(root)
   (folder/'manifest.json').write_text('[]');(folder/'COMPLETE').write_text(json.dumps(st.digest(folder/'manifest.json')))
   with patch.object(st,'recovery_point',return_value=point()):result=bc.business_coverage(report,Mock())
   self.assertEqual(result['status'],'unknown');self.assertEqual(report['entries'][0]['businessCoverage'],'unavailable')
 def test_unavailable_database_is_not_reported_as_covered(self):
  with patch.object(st,'recovery_point',side_effect=st.StorageError('connection failed')):
   self.assertEqual(bc.business_coverage({'entries':[]},Mock())['status'],'unavailable')
 def test_unknown_future_table_still_refused(self):
  with tempfile.TemporaryDirectory() as temp:
   folder=recorded(Path(temp),point());manifest=json.loads((folder/'manifest.json').read_text());manifest['counts']['unknown_future_table']=1
   (folder/'manifest.json').write_text(json.dumps(manifest));(folder/'COMPLETE').write_text(json.dumps(st.digest(folder/'manifest.json')))
   self.assertFalse(bc.inspect(folder)['metadataValid'])

if __name__=='__main__':unittest.main()
