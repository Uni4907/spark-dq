import contextlib,io,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import cluster_lab as lab
class LabTest(unittest.TestCase):
 def fixture(self,temp):
  base=Path(temp);root=base/'.runtime/lab';root.mkdir(parents=True);(root/'compose.yaml').write_text('services: {}\n');(root/'.env').write_text('LAB_PROJECT=spark-dq-lab-123456789abc\n')
  node={'project':'spark-dq-lab-123456789abc','configSha256':lab.st.digest(root/'compose.yaml')['sha256'],'envSha256':lab.st.digest(root/'.env')['sha256']};(root/'created.json').write_text(json.dumps(node));return base,root,node
 def test_modified_control_files_refuse_before_docker(self):
  with tempfile.TemporaryDirectory() as temp:
   base,root,node=self.fixture(temp);(root/'compose.yaml').write_text('changed')
   with patch.object(lab.st,'ROOT',base),patch.object(lab.st,'run') as run,self.assertRaises(AssertionError):lab.stop(root)
   run.assert_not_called()
 def test_low_host_capacity_cannot_create_private_files_or_touch_docker(self):
  with tempfile.TemporaryDirectory() as temp:
   base=Path(temp);(base/'.runtime').mkdir();capacity=base/'capacity.json';capacity.write_text('{"known":true,"freeBytes":1}')
   with patch.object(lab.st,'ROOT',base),patch.object(lab.st,'run') as run,self.assertRaises(AssertionError):lab.create(base/'.runtime/new',capacity)
   self.assertFalse((base/'.runtime/new').exists());run.assert_not_called()
 def test_foreign_container_ownership_blocks_actions(self):
  with tempfile.TemporaryDirectory() as temp:
   base,root,node=self.fixture(temp)
   with patch.object(lab.st,'run',side_effect=[b'foreign\n',b'other-owner']) as run,self.assertRaises(AssertionError):lab.owned(root,node,None)
   self.assertEqual(run.call_count,2)
 def test_stale_pid_cannot_receive_stop_signal(self):
  with tempfile.TemporaryDirectory() as temp:
   base,root,node=self.fixture(temp);(root/'state.json').write_text(json.dumps({'pid':1234,'startTicks':'old','state':'running'}))
   with patch.object(lab.st,'ROOT',base),patch.object(lab.rt,'ticks',return_value='new'),patch.object(lab.os,'kill') as kill:
    self.assertFalse(lab.stop(root)['alive']);kill.assert_not_called()
 def test_other_lab_process_cannot_receive_stop_signal(self):
  with tempfile.TemporaryDirectory() as temp:
   base,root,node=self.fixture(temp);(root/'state.json').write_text(json.dumps({'pid':1234,'startTicks':'same','state':'running'}));command=b'python3\0'+str(Path(lab.__file__).resolve()).encode()+b'\0serve\0--output\0/another/lab\0'
   with patch.object(lab.st,'ROOT',base),patch.object(lab.rt,'ticks',return_value='same'),patch.object(Path,'read_bytes',return_value=command),patch.object(lab.os,'kill') as kill:
    self.assertFalse(lab.stop(root)['alive']);kill.assert_not_called()
 def test_explicit_project_and_cleared_ambient_overrides(self):
  with tempfile.TemporaryDirectory() as temp:
   base,root,node=self.fixture(temp)
   with patch.object(lab.st,'ROOT',base),patch.dict(lab.os.environ,{'LAB_PROJECT':'spark-dq','MYSQL_PASSWORD':'do-not-leak'}):
    _,_,project=lab.load(root);self.assertEqual(project.compose[-2:],['--project-name',node['project']]);self.assertIn('LAB_PROJECT',project.compose);self.assertNotIn('do-not-leak',str(project.compose));self.assertEqual(project.compose[0],'env')
 def test_outage_always_resumes_after_interruption(self):
  from unittest.mock import Mock
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);node={'project':'spark-dq-lab-123456789abc'};project=Mock();project.service.return_value='owned-master'
   with patch.object(lab,'load',return_value=(root,node,project)),patch.object(lab,'owned'),patch.object(lab.rt,'active',return_value=0),patch.object(lab.signal,'signal'),patch.object(lab.st,'run',side_effect=[b'false',b'ok',b'ok']) as run,patch.object(lab.time,'monotonic',return_value=0),patch.object(lab.time,'sleep',side_effect=RuntimeError('interrupted')),self.assertRaises(RuntimeError):lab.outage(root,1)
   self.assertEqual(run.call_args.args[0],['docker','unpause','owned-master'])
 def test_outage_never_takes_over_an_already_paused_master(self):
  from unittest.mock import Mock
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);node={'project':'spark-dq-lab-123456789abc'};project=Mock();project.service.return_value='owned-master'
   with patch.object(lab,'load',return_value=(root,node,project)),patch.object(lab,'owned'),patch.object(lab.rt,'active',return_value=0),patch.object(lab.st,'run',return_value=b'true') as run,self.assertRaises(AssertionError):lab.outage(root,1)
   self.assertEqual(run.call_count,1)
class LabImagesTest(unittest.TestCase):
 def test_defaults_resolve_local_images_to_immutable_ids(self):
  with patch.object(lab.st,'run',return_value=('sha256:'+'a'*64).encode()) as run:
   references,images=lab.resolve_images()
   self.assertEqual(set(images),{'backend','frontend','worker'});self.assertEqual(references['backend'],'spark-dq-backend:cluster-ui')
   self.assertEqual(run.call_count,3);self.assertTrue(all(call.args[0][:3]==['docker','image','inspect'] for call in run.call_args_list))
 def test_override_leaves_existing_tags_and_other_roles_untouched(self):
  with patch.object(lab.st,'run',return_value=('sha256:'+'a'*64).encode()) as run:
   refs,images=lab.resolve_images({'backend':'spark-dq-backend:header-candidate'})
   self.assertEqual(refs['backend'],'spark-dq-backend:header-candidate');self.assertEqual(refs['worker'],'spark-dq-worker:cluster-ui')
   self.assertEqual(run.call_args_list[0].args[0][-1],refs['backend']);self.assertTrue(all(v=='sha256:'+'a'*64 for v in images.values()))
 def test_invalid_references_reject_before_any_docker_action(self):
  for overrides in ({'other':'x'},{'backend':'--help'},{'backend':'x\nsecret'},{'backend':'$(touch bad)'},{'worker':None}):
   with patch.object(lab.st,'run') as run,self.assertRaises(ValueError):lab.resolve_images(overrides)
   run.assert_not_called()
 def test_inspection_must_return_full_immutable_digest(self):
  with patch.object(lab.st,'run',return_value=b'spark-dq-backend:tag'),self.assertRaises(ValueError):lab.resolve_images()
class LabSubnetsTest(unittest.TestCase):
 def test_internal_and_edge_subnets_are_distinct_and_avoid_existing_routes(self):
  import ipaddress
  used=['10.240.240.0/28','192.168.0.0/16','172.20.0.0/16','default']
  selected=lab.allocate_subnets(used);self.assertNotEqual(selected['default'],selected['edge'])
  self.assertTrue(all(ipaddress.ip_network(value).prefixlen==28 for value in selected.values()))
  self.assertFalse(any(ipaddress.ip_network(value).overlaps(ipaddress.ip_network(other)) for value in selected.values() for other in used if other!='default'))
 def test_no_available_subnet_fails_without_mutation(self):
  with self.assertRaises(ValueError):lab.allocate_subnets(['10.0.0.0/8','192.168.0.0/16'])
 def test_network_inventory_handles_null_ipam_and_host_routes(self):
  with patch.object(lab.st,'run',side_effect=[b'host\nbridge',b'[{"IPAM":{"Config":null}},{"IPAM":{"Config":[{"Subnet":"10.240.240.0/28"}]}}]',b'[{"dst":"10.240.240.16/28"},{"dst":"default"}]']):
   plan=lab.network_plan();self.assertNotIn(plan['default'],['10.240.240.0/28','10.240.240.16/28']);self.assertEqual(len(plan['observedSubnets']),3)
class LabAddressTest(unittest.TestCase):
 def test_running_status_refreshes_ports_after_compose_restart(self):
  state={'alive':True,'state':'running','api':'http://127.0.0.1:111/api','web':'http://127.0.0.1:112'}
  with patch.object(lab,'address',side_effect=['http://127.0.0.1:221','http://127.0.0.1:222']):
   observed=lab.observed_addresses(state,None);self.assertEqual(observed['api'],'http://127.0.0.1:221/api');self.assertEqual(observed['web'],'http://127.0.0.1:222');self.assertEqual(observed['addressObservation'],'OBSERVED')
 def test_unobservable_ports_never_return_stale_urls_as_current(self):
  with patch.object(lab,'address',side_effect=lab.st.StorageError('unavailable')):
   observed=lab.observed_addresses({'alive':True,'state':'running','api':'old','web':'old'},None)
   self.assertNotIn('api',observed);self.assertNotIn('web',observed);self.assertEqual(observed['addressObservation'],'UNAVAILABLE')
 def test_stale_controller_never_probes_ports(self):
  with patch.object(lab,'address') as address:lab.observed_addresses({'alive':False,'state':'running'},None)
  address.assert_not_called()
if __name__=='__main__':unittest.main()
