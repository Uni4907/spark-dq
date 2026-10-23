import json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import standalone_trial as t

class EvidenceTest(unittest.TestCase):
 def proof(self,path,rows=None,**changes):
  node={'version':1,'applicationEnded':True,'truncated':False,'executors':rows if rows is not None else [{'id':'0','host':'worker2','successfulTasks':120,'failedTasks':0},{'id':'1','host':'worker1','successfulTasks':129,'failedTasks':0}]};node.update(changes);path.write_text(json.dumps(node));return node
 def test_actual_hostname_evidence_binds_two_remote_workers(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'proof.json';self.proof(path);result=t.evidence(path,{'worker1','worker2'});self.assertEqual(sum(row['successfulTasks'] for row in result['executors']),249)
 def test_one_worker_or_local_driver_is_not_distributed_proof(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'proof.json'
   for rows in ([{'id':'driver','host':'worker1','successfulTasks':9,'failedTasks':0}],[{'id':'0','host':'worker1','successfulTasks':9,'failedTasks':0}]):
    self.proof(path,rows)
    with self.assertRaises(AssertionError):t.evidence(path,{'worker1','worker2'})
 def test_recheck_can_prove_remote_execution_without_claiming_two_workers(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'proof.json';self.proof(path,[{'id':'1','host':'worker2','successfulTasks':235,'failedTasks':0}])
   self.assertEqual(len(t.evidence(path,{'worker1','worker2'},minimum_workers=1)['executors']),1)
   with self.assertRaises(AssertionError):t.evidence(path,{'worker1','worker2'})
 def test_failed_tasks_truncated_or_unfinished_evidence_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'proof.json'
   for changes in ({'truncated':True},{'applicationEnded':False}):
    self.proof(path,**changes)
    with self.assertRaises(AssertionError):t.evidence(path,{'worker1','worker2'})
   node=self.proof(path);node['executors'][0]['failedTasks']=1;path.write_text(json.dumps(node))
   with self.assertRaises(AssertionError):t.evidence(path,{'worker1','worker2'})
 def test_duplicate_executor_id_or_boolean_failure_count_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'proof.json';node=self.proof(path);node['executors'][1]['id']='0';path.write_text(json.dumps(node))
   with self.assertRaises(AssertionError):t.evidence(path,{'worker1','worker2'})
   node=self.proof(path);node['executors'][0]['failedTasks']=False;path.write_text(json.dumps(node))
   with self.assertRaises(AssertionError):t.evidence(path,{'worker1','worker2'})
 def test_unexpected_host_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'proof.json';self.proof(path)
   with self.assertRaises(AssertionError):t.evidence(path,{'other1','other2'})
 def test_process_check_includes_required_docker_pid_column(self):
  with tempfile.TemporaryDirectory() as tmp:
   cluster=t.Cluster(Path(tmp),'session');cluster.workers=['one','two']
   with patch.object(t.st,'run',return_value=b'PID COMMAND\n1 master') as run:
    self.assertTrue(cluster.no_executors());self.assertEqual(run.call_args.args[0][-2:],['-eo','pid,args'])
   with patch.object(t.st,'run',return_value=b'PID COMMAND\n1 CoarseGrainedExecutorBackend'):
    self.assertFalse(cluster.no_executors())
 def test_owned_empty_network_removed_after_stopping_nodes(self):
  with tempfile.TemporaryDirectory() as tmp:
   cluster=t.Cluster(Path(tmp),'session');cluster.network_created=True
   with patch.object(t.st,'run',side_effect=[json.dumps([{'Labels':{t.LABEL:'session'},'Containers':{}}]).encode(),b'network']) as run:cluster.close()
   self.assertFalse(cluster.network_created);self.assertEqual(run.call_args.args[0],['docker','network','rm',cluster.network])
 def test_network_ownership_or_live_endpoint_refuses_removal(self):
  for info in ({'Labels':{t.LABEL:'other'},'Containers':{}},{'Labels':{t.LABEL:'session'},'Containers':{'outside':{}}}):
   with tempfile.TemporaryDirectory() as tmp:
    cluster=t.Cluster(Path(tmp),'session');cluster.network_created=True
    with patch.object(t.st,'run',return_value=json.dumps([info]).encode()) as run,self.assertRaises(AssertionError):cluster.close()
    self.assertEqual(run.call_count,1)
 def test_ownership_mismatch_never_stops_container(self):
  with tempfile.TemporaryDirectory() as tmp:
   cluster=t.Cluster(Path(tmp),'my-session');cluster.containers=[('worker1','container')]
   with patch.object(t.st,'run',return_value=b'other-session') as run,self.assertRaises(AssertionError):cluster.close()
   self.assertEqual(len(run.call_args_list),1);self.assertNotIn('stop',run.call_args.args[0])
 def test_owned_container_stops_bounded_and_retains_logs(self):
  with tempfile.TemporaryDirectory() as tmp:
   cluster=t.Cluster(Path(tmp),'session');cluster.containers=[('worker1','container')]
   with patch.object(t.st,'run',side_effect=[b'session',b'true',b'container']) as run,patch.object(cluster,'logs') as logs:cluster.close()
   self.assertEqual(run.call_args.args[0],['docker','stop','--time','5','container']);self.assertEqual(run.call_args.kwargs['timeout'],20);logs.assert_called_once()
if __name__=='__main__':unittest.main()
