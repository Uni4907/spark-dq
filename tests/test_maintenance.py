"""Maintenance coordinator fault paths; no Docker or real credentials."""
import json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'infrastructure'))
import maintenance as m
import storage as st

class CoordinatorTest(unittest.TestCase):
    def test_temporary_name_collision_preserves_existing_file(self):
        with tempfile.TemporaryDirectory() as temp,patch.object(m.uuid,'uuid4',return_value=Mock(hex='collision')):
            path=Path(temp);existing=path/'.receipt-collision.tmp';existing.write_text('user-data')
            with self.assertRaises(FileExistsError):m.persist(path/'receipt.json',{})
            self.assertEqual(existing.read_text(),'user-data')

    def setup_project(self):
        project=Mock()
        project.compose=['docker','compose','--env-file','/env','-f','/config']
        project.service.return_value='container';project.running.return_value=True
        control=Mock();control.healthy.return_value=True
        control.api.return_value={'maintenance':False,'owner':''}
        return project,control
    def test_existing_output_refuses_before_touching_service(self):
        p,c=self.setup_project()
        with tempfile.TemporaryDirectory() as temp,patch.object(m,'Control') as control:
            path=Path(temp);(path/'user.txt').write_text('keep')
            with self.assertRaises(st.StorageError):m.backup_once(p,path)
            self.assertEqual((path/'user.txt').read_text(),'keep');control.assert_not_called();p.running.assert_not_called()
    def test_stopped_backend_is_not_started(self):
        p,c=self.setup_project();p.running.return_value=False
        with tempfile.TemporaryDirectory() as temp,patch.object(st,'run') as run:
            with self.assertRaises(st.StorageError):m.backup_once(p,Path(temp)/'out')
            run.assert_not_called()
    def test_capacity_failure_does_not_close_gate(self):
        p,c=self.setup_project()
        with tempfile.TemporaryDirectory() as temp,patch.object(m,'Control',return_value=c),patch.object(m,'estimate',side_effect=st.StorageError('low space')),patch.object(st,'run') as run:
            output=Path(temp)/'out'
            with self.assertRaises(st.StorageError):m.backup_once(p,output)
            self.assertFalse(output.exists());run.assert_not_called()
            self.assertEqual(c.api.call_args_list[0].args,())
            self.assertEqual(c.api.call_count,1)
    def test_busy_entry_recovers_lost_response_without_stopping(self):
        p,c=self.setup_project()
        def api(action='',owner=None):
            if action=='/enter':raise st.StorageError('busy')
            return {'maintenance':False,'owner':''}
        c.api.side_effect=api
        with tempfile.TemporaryDirectory() as temp,patch.object(m,'Control',return_value=c),patch.object(m,'estimate',return_value={}),patch.object(st,'run') as run,patch.object(st,'backup') as backup:
            output=Path(temp)/'out'
            with self.assertRaises(st.StorageError):m.backup_once(p,output)
            run.assert_not_called();backup.assert_not_called()
            receipt=json.loads((output/'receipt.json').read_text())
            self.assertTrue(receipt['maintenanceReleased']);self.assertFalse(receipt['needsResume'])
    def test_backup_failure_restarts_and_releases(self):
        p,c=self.setup_project();p.running.side_effect=[True,True,False]
        with tempfile.TemporaryDirectory() as temp,patch.object(m,'Control',return_value=c),patch.object(m,'estimate',return_value={}),patch.object(st,'run') as run,patch.object(st,'backup',side_effect=st.StorageError('dump failed')):
            output=Path(temp)/'out'
            with self.assertRaises(st.StorageError):m.backup_once(p,output)
            self.assertTrue(any(call.args[0][-2:]==['start','backend'] for call in run.call_args_list))
            receipt=json.loads((output/'receipt.json').read_text())
            self.assertTrue(receipt['serviceResumed']);self.assertFalse(receipt['needsResume'])
            self.assertEqual(receipt['state'],'failed')
    def test_recovery_failure_keeps_receipt_for_resume(self):
        p,c=self.setup_project()
        def api(action='',owner=None):
            if action=='/leave':raise st.StorageError('leave unavailable')
            return {'maintenance':False,'owner':''}
        c.api.side_effect=api
        with tempfile.TemporaryDirectory() as temp,patch.object(m,'Control',return_value=c),patch.object(m,'estimate',return_value={}),patch.object(st,'run'),patch.object(st,'backup',return_value={}),patch.object(st,'verify'):
            output=Path(temp)/'out'
            with self.assertRaises(st.StorageError):m.backup_once(p,output)
            receipt=json.loads((output/'receipt.json').read_text())
            self.assertTrue(receipt['needsResume']);self.assertFalse(receipt['maintenanceReleased'])
    def test_success_verifies_and_receipt_never_contains_secret(self):
        p,c=self.setup_project();c.key='private-not-for-report'
        with tempfile.TemporaryDirectory() as temp,patch.object(m,'Control',return_value=c),patch.object(m,'estimate',return_value={}),patch.object(st,'run'),patch.object(st,'backup',return_value={'counts':{}}),patch.object(st,'verify') as verify:
            output=Path(temp)/'out';result=m.backup_once(p,output)
            verify.assert_called_once_with(output/'backup')
            self.assertEqual(result['state'],'complete');self.assertFalse(result['needsResume'])
            self.assertNotIn(c.key,(output/'receipt.json').read_text())
            self.assertEqual(output.stat().st_mode & 0o777,0o700)
            self.assertEqual((output/'receipt.json').stat().st_mode & 0o777,0o600)
    def test_interruption_attempts_service_recovery(self):
        p,c=self.setup_project()
        with tempfile.TemporaryDirectory() as temp,patch.object(m,'Control',return_value=c),patch.object(m,'estimate',return_value={}),patch.object(st,'run'),patch.object(st,'backup',side_effect=InterruptedError('signal')):
            output=Path(temp)/'out'
            with self.assertRaises(st.StorageError):m.backup_once(p,output)
            receipt=json.loads((output/'receipt.json').read_text())
            self.assertEqual(receipt['failureType'],'InterruptedError');self.assertTrue(receipt['maintenanceReleased'])
    def test_changed_container_is_never_started_or_unlocked(self):
        p,c=self.setup_project()
        with tempfile.TemporaryDirectory() as temp,patch.object(st,'run') as run,patch.object(m,'Control') as control:
            with self.assertRaises(st.StorageError):m.recover(p,{'backendContainerId':'other'},Path(temp)/'receipt')
            run.assert_not_called();control.assert_not_called()
    def test_different_owner_is_never_released(self):
        p,c=self.setup_project();c.api.return_value={'maintenance':True,'owner':'other'}
        with tempfile.TemporaryDirectory() as temp,patch.object(m,'Control',return_value=c):
            with self.assertRaises(st.StorageError):m.recover(p,{'backendContainerId':'container','owner':'ours'},Path(temp)/'receipt')
            self.assertEqual(c.api.call_count,1)

if __name__=='__main__':unittest.main()
