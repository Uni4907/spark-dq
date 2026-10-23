"""Pure stdlib integrity/boundary tests; no Docker or user data touched."""
import contextlib, hashlib, importlib.util, io, json, tarfile, tempfile, unittest
from pathlib import Path
from unittest.mock import Mock, patch

spec=importlib.util.spec_from_file_location('storage',Path(__file__).resolve().parent.parent/'infrastructure/storage.py')
storage=importlib.util.module_from_spec(spec);spec.loader.exec_module(storage)

def archive(entries):
    stream=io.BytesIO()
    with tarfile.open(fileobj=stream,mode='w') as tar:
        for name,data,kind in entries:
            entry=tarfile.TarInfo(name);entry.type=kind
            if kind==tarfile.REGTYPE:
                entry.size=len(data);tar.addfile(entry,io.BytesIO(data))
            else:
                entry.linkname='raw/target.csv';tar.addfile(entry)
    stream.seek(0);return stream

def metadata():
    return {'counts':{'datasets':1},'datasets':[{'id':'sample','path':'/data/raw/sample.csv','sha256':hashlib.sha256(b'x').hexdigest(),'size':1}],
      'tasks':[{'id':'task','status':'SUCCEEDED'},{'id':'failed','status':'FAILED'}],
      'results':[{'taskId':'task','ruleId':'rule','status':'SUCCEEDED','path':'/data/tasks/task/anomalies/rule'}],
      'snapshotMismatches':[],'missingResults':[]}

class StorageTest(unittest.TestCase):
    def test_inventory_accepts_legacy_or_migrated_tables_only(self):
        for history in (False,True):
            p=storage.Project();tables=list(storage.TABLES)+(['flyway_schema_history'] if history else [])
            p.mysql=Mock(side_effect=[('\n'.join(tables)+'\n').encode()]+[b'0']*len(tables))
            p.rows=Mock(return_value=[])
            result=p.inventory()
            self.assertEqual(set(result['counts']),set(tables))
        p=storage.Project();p.mysql=Mock(return_value=('\n'.join(storage.TABLES)+'\nunknown_table\n').encode())
        with self.assertRaises(storage.StorageError):p.inventory()
    def test_streaming_hash_unicode_and_zero_anomaly_files(self):
        raw=archive([('./raw/中文.csv',b'abc',tarfile.REGTYPE),('./tasks/t/anomalies/r/part-0.json',b'',tarfile.REGTYPE)])
        result=storage.scan_tar(raw)
        self.assertEqual(result['bytes'],3)
        self.assertEqual(result['files']['raw/中文.csv']['sha256'],hashlib.sha256(b'abc').hexdigest())
        self.assertEqual(result['files']['tasks/t/anomalies/r/part-0.json']['size'],0)
    def test_traversal_and_absolute_names_rejected(self):
        for name in ('../escape','raw/../../escape','/absolute','raw/../escape'):
            with self.subTest(name=name),self.assertRaises(storage.StorageError):
                storage.scan_tar(archive([(name,b'x',tarfile.REGTYPE)]))
    def test_links_and_devices_rejected(self):
        for kind in (tarfile.SYMTYPE,tarfile.LNKTYPE,tarfile.FIFOTYPE,tarfile.CHRTYPE):
            with self.subTest(kind=kind),self.assertRaises(storage.StorageError):
                storage.scan_tar(archive([('raw/link',b'',kind)]))
    def test_normalized_duplicate_rejected(self):
        with self.assertRaises(storage.StorageError):
            storage.scan_tar(archive([('raw/a',b'a',tarfile.REGTYPE),('./raw/a',b'b',tarfile.REGTYPE)]))
    def test_archive_size_and_entry_limits(self):
        with patch.object(storage,'MAX_BYTES',2),self.assertRaises(storage.StorageError):
            storage.scan_tar(archive([('raw/a',b'abc',tarfile.REGTYPE)]))
        with patch.object(storage,'MAX_MEMBERS',1),self.assertRaises(storage.StorageError):
            storage.scan_tar(archive([('raw/a',b'a',tarfile.REGTYPE),('raw/b',b'b',tarfile.REGTYPE)]))
    def test_audit_distinguishes_zero_results_diagnostics_and_orphans(self):
        files=storage.scan_tar(archive([('raw/sample.csv',b'x',tarfile.REGTYPE),
          ('tasks/task/anomalies/rule/part-0.json',b'',tarfile.REGTYPE),
          ('tasks/failed/request.json',b'{}',tarfile.REGTYPE),('raw/orphan.csv',b'q',tarfile.REGTYPE)]))
        report=storage.compare(metadata(),files)
        self.assertTrue(report['ok']);self.assertEqual(report['issues'],[])
        self.assertEqual(report['warnings'],[{'kind':'unregisteredRawFile','path':'raw/orphan.csv'}])
    def test_audit_detects_missing_changed_invalid_and_partial_results(self):
        m=metadata();m['datasets'][0]['path']='/elsewhere'
        m['results'][0]['status']='FAILED';m['results'][0]['path']='/elsewhere'
        m['snapshotMismatches']=[{'taskId':'task'}];m['missingResults']=[{'ruleId':'other','taskId':'task'}]
        result=storage.compare(m,{'files':{},'bytes':0})
        kinds={r['kind'] for r in result['issues']}
        self.assertEqual(kinds,{'invalidDatasetPath','missingDatasetFile','resultOnUnsuccessfulTask',
          'invalidAnomalyPath','missingAnomalyParts','snapshotFingerprintMismatch','successfulRuleWithoutResult'})
        result=storage.compare(metadata(),{'files':{'raw/sample.csv':{'size':1,'sha256':'0'*64}},'bytes':1})
        self.assertIn('changedDatasetFile',{r['kind'] for r in result['issues']})
    def make_backup(self,folder):
        (folder/'database.sql').write_bytes(b'SELECT 1;\n')
        (folder/'files.tar').write_bytes(archive([('raw/a',b'x',tarfile.REGTYPE)]).getvalue())
        (folder/'application.tar').write_bytes(archive([('backend.jar',b'jar',tarfile.REGTYPE),('engine/spark-engine-0.1.0.jar',b'engine',tarfile.REGTYPE),('engine/dependency/example.jar',b'lib',tarfile.REGTYPE)]).getvalue())
        storage.save_json(folder/'audit.json',{'ok':True})
        manifest={'formatVersion':'sparkdq-backup-v1','mysqlImage':storage.MYSQL_IMAGE,'backendImageId':'sha256:'+'a'*64,
          'counts':{},'artifacts':{n:storage.digest(folder/n) for n in ('database.sql','files.tar','application.tar','audit.json')}}
        storage.save_json(folder/'manifest.json',manifest)
        storage.save_json(folder/'COMPLETE',storage.digest(folder/'manifest.json'))
        return manifest
    def test_complete_backup_verifies_but_sql_corruption_never_reaches_docker(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp);expected=self.make_backup(folder)
            self.assertEqual(storage.verify(folder),expected)
            (folder/'database.sql').write_bytes(b'changed')
            with patch.object(storage,'run') as docker,self.assertRaises(storage.StorageError):
                storage.restore(folder,folder/'restore')
            docker.assert_not_called();self.assertFalse((folder/'restore').exists())
    def test_missing_completion_marker_and_manifest_change_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp)
            with self.assertRaises(storage.StorageError):storage.verify(folder)
            self.make_backup(folder)
            (folder/'manifest.json').write_text('{}')
            with self.assertRaises(storage.StorageError):storage.verify(folder)
    def test_restore_rejects_hashed_unsafe_archive_before_any_docker_operation(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp);manifest=self.make_backup(folder)
            (folder/'files.tar').write_bytes(archive([('../escape',b'x',tarfile.REGTYPE)]).getvalue())
            manifest['artifacts']['files.tar']=storage.digest(folder/'files.tar')
            (folder/'manifest.json').write_text(json.dumps(manifest))
            (folder/'COMPLETE').write_text(json.dumps(storage.digest(folder/'manifest.json')))
            with patch.object(storage,'run') as docker,self.assertRaises(storage.StorageError):
                storage.restore(folder,folder/'restore')
            docker.assert_not_called()
    def test_existing_backup_directory_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp);(folder/'user-file').write_text('preserve')
            project=Mock()
            with self.assertRaises(FileExistsError):storage.backup(project,folder)
            project.inventory.assert_not_called();self.assertEqual((folder/'user-file').read_text(),'preserve')
    def test_application_bundle_requires_engine_and_rejects_non_jar_secrets(self):
        with self.assertRaises(storage.StorageError):
            storage.application_inventory(archive([('backend.jar',b'jar',tarfile.REGTYPE)]))
        with self.assertRaises(storage.StorageError):
            storage.application_inventory(archive([('backend.jar',b'jar',tarfile.REGTYPE),
              ('engine/spark-engine-0.1.0.jar',b'engine',tarfile.REGTYPE),
              ('engine/dependency/lib.jar',b'lib',tarfile.REGTYPE),('engine/.env',b'private',tarfile.REGTYPE)]))
    def test_project_config_paths_follow_caller_directory(self):
        with tempfile.TemporaryDirectory() as temp,contextlib.chdir(temp):
            project=storage.Project(Path('isolated/compose.yaml'),Path('isolated/.env'))
            self.assertEqual(project.compose[3],str(Path(temp)/'isolated/.env'))
            self.assertEqual(project.compose[5],str(Path(temp)/'isolated/compose.yaml'))
    def test_exclusive_json_write_preserves_prior_file(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'report.json';storage.save_json(path,{'first':1})
            with self.assertRaises(FileExistsError):storage.save_json(path,{'second':2})
            self.assertEqual(json.loads(path.read_text()),{'first':1})

if __name__=='__main__':unittest.main()
