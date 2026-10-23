"""Operational safety checks with no running Docker required."""
import contextlib,datetime as dt,hashlib,json,os,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'infrastructure'))
import backup_catalog as bc
import runtime as rt
import storage as st

def backup(root,name='backup',hours=0):
    folder=root/name;folder.mkdir()
    artifacts={}
    for filename in bc.ARTIFACTS:
        file=folder/filename;file.write_bytes(b'abc')
        artifacts[filename]=st.digest(file)
    st.save_json(folder/'manifest.json',{'formatVersion':'sparkdq-backup-v1',
      'createdAt':(dt.datetime.now(bc.UTC)-dt.timedelta(hours=hours)).isoformat(),
      'counts':dict.fromkeys(st.TABLES,0),'artifacts':artifacts})
    st.save_json(folder/'COMPLETE',st.digest(folder/'manifest.json'))
    return folder

class CatalogTest(unittest.TestCase):
    def test_unreadable_directory_is_reported_as_incomplete_scan(self):
        with tempfile.TemporaryDirectory() as temp:
            def walk(root,**kwargs):
                kwargs['onerror'](PermissionError(13,'denied',str(root/'private')))
                return iter([])
            with patch.object(bc.os,'walk',side_effect=walk):
                result=bc.catalog(Path(temp))
            self.assertFalse(result['scanComplete'])
            self.assertEqual(result['scanErrors'][0]['type'],'PermissionError')

    def test_metadata_does_not_claim_full_verification(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=backup(Path(temp))
            with patch.object(st,'verify') as verify:
                result=bc.catalog(Path(temp))
            verify.assert_not_called()
            self.assertEqual(result['entries'][0]['verification'],'not-run')
            self.assertFalse(result['freshVerifiedBackup'])
            self.assertEqual(result['actionsPerformed'],[])
    def test_incomplete_and_symlink_artifacts_rejected_without_deletion(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);folder=backup(root)
            (folder/'COMPLETE').rename(folder/'incomplete-marker')
            self.assertFalse(bc.inspect(folder)['metadataValid'])
            (folder/'incomplete-marker').rename(folder/'COMPLETE')
            (folder/'files.tar').rename(folder/'real-file')
            (folder/'files.tar').symlink_to(folder/'real-file')
            self.assertFalse(bc.inspect(folder)['metadataValid'])
            self.assertEqual((folder/'real-file').read_bytes(),b'abc')
    def test_same_size_corruption_needs_full_hash_and_is_detected(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);folder=backup(root)
            (folder/'database.sql').write_bytes(b'xyz')
            self.assertTrue(bc.inspect(folder)['metadataValid'])
            result=bc.catalog(root,verify_latest=1)
            self.assertEqual(result['entries'][0]['verification'],'failed')
            self.assertFalse(result['freshVerifiedBackup'])
            self.assertEqual((folder/'database.sql').read_bytes(),b'xyz')
    def test_failed_newest_falls_back_to_older_verified_backup(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);bad=backup(root,'bad',0);good=backup(root,'good',1)
            def verify(folder):
                if folder==bad:raise st.StorageError('corrupt')
                return {}
            with patch.object(st,'verify',side_effect=verify):
                result=bc.catalog(root,verify_latest=1)
            self.assertEqual(result['latestVerified'],str(good))
            self.assertTrue(result['freshVerifiedBackup'])
            self.assertEqual(next(e for e in result['entries'] if e['path']==str(bad))['retentionAdvice'],'inspect')
    def test_age_and_space_warnings(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);backup(root,hours=48)
            with patch.object(st,'verify',return_value={}),patch.object(bc.shutil,'disk_usage',return_value=Mock(free=100,total=10000)):
                result=bc.catalog(root,verify_latest=1)
            self.assertFalse(result['freshVerifiedBackup'])
            self.assertFalse(result['disk']['enoughForEstimate'])
    def test_retention_advice_preserves_all_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            for n in range(4):backup(root,'b'+str(n),n)
            before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
            result=bc.catalog(root,keep=2)
            self.assertEqual([e['retentionAdvice'] for e in result['entries']],['keep','keep','review-only','review-only'])
            after={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
            self.assertEqual(before,after)
    def test_unknown_table_layout_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=backup(Path(temp));data=json.loads((folder/'manifest.json').read_text())
            data['counts']['unexpected']=1
            (folder/'manifest.json').write_text(json.dumps(data))
            (folder/'COMPLETE').write_text(json.dumps(st.digest(folder/'manifest.json')))
            self.assertFalse(bc.inspect(folder)['metadataValid'])

class SessionTest(unittest.TestCase):
    def test_wall_clock_forward_expires_even_when_monotonic_has_not_advanced(self):
        with patch.object(rt.time,'time',return_value=200),patch.object(rt.time,'monotonic',return_value=10):
            self.assertTrue(rt.reached(100,100))
    def test_monotonic_bounds_backward_wall_clock_change(self):
        with patch.object(rt.time,'time',return_value=10),patch.object(rt.time,'monotonic',return_value=200):
            self.assertTrue(rt.reached(100,100))

    def test_missing_and_stale_state_do_not_claim_running(self):
        with tempfile.TemporaryDirectory() as temp,patch.object(rt,'STATE',Path(temp)/'state'):
            self.assertFalse(rt.read_state()['alive'])
            rt.write_state({'state':'running','pid':999999999,'startTicks':'1'})
            self.assertEqual(rt.read_state()['state'],'stale')
    def test_atomic_private_state(self):
        with tempfile.TemporaryDirectory() as temp,patch.object(rt,'STATE',Path(temp)/'state'):
            rt.write_state({'state':'stopped'})
            self.assertEqual(rt.STATE.stat().st_mode & 0o777,0o600)
            self.assertEqual(len(list(Path(temp).iterdir())),1)
    def test_current_unrelated_python_process_is_not_our_session(self):
        self.assertFalse(rt.belongs({'pid':os.getpid(),'startTicks':rt.ticks(os.getpid())}))
    def test_busy_stop_never_signals_or_cancels(self):
        with patch.object(rt,'read_state',return_value={'alive':True,'pid':123}),patch.object(rt,'active',return_value=1),patch.object(rt.os,'kill') as kill:
            with self.assertRaises(st.StorageError):rt.stop()
            kill.assert_not_called()
    def test_stopped_session_stop_does_not_touch_docker(self):
        with patch.object(rt,'read_state',return_value={'alive':False,'state':'stopped'}),patch.object(st,'Project') as project:
            self.assertFalse(rt.stop()['alive'])
            project.assert_not_called()
    def test_duplicate_session_does_not_start_or_renew_services(self):
        with tempfile.TemporaryDirectory() as temp,patch.object(rt,'LOCK',Path(temp)/'lock'),patch.object(rt.fcntl,'flock',side_effect=BlockingIOError),patch.object(rt,'read_state',return_value={'alive':True,'sessionId':'existing','durationSeconds':30}),patch.object(st,'Project') as project:
            result=rt.serve(100)
            self.assertEqual(result['sessionId'],'existing')
            self.assertEqual(result['durationSeconds'],30)
            project.assert_not_called()
    def test_startup_failure_keeps_diagnostics_without_container_stop(self):
        with tempfile.TemporaryDirectory() as temp,patch.object(rt,'STATE',Path(temp)/'state'),patch.object(rt,'LOCK',Path(temp)/'lock'),patch.object(rt.signal,'signal'),patch.object(st,'lock',return_value=contextlib.nullcontext()),patch.object(st,'Project',return_value=Mock(running=Mock(return_value=False),compose=['docker','compose'])),patch.object(st,'run',side_effect=st.StorageError('startup failure')) as run:
            with self.assertRaises(st.StorageError):rt.serve(20)
            self.assertEqual(json.loads(rt.STATE.read_text())['state'],'failed')
            self.assertEqual(len(run.call_args_list),1)
            self.assertNotIn('stop',run.call_args.args[0])

if __name__=='__main__':unittest.main()
