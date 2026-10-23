import sys,tempfile,unittest,json
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import cluster_api_trial as trial
class NodesTest(unittest.TestCase):
 def test_foreign_paused_node_is_never_unpaused(self):
  with tempfile.TemporaryDirectory() as tmp:
   nodes=trial.Nodes(Path(tmp),'network','files');nodes.paused={'foreign'}
   with patch.object(trial.st,'run',return_value=b'other-session') as run,self.assertRaises(AssertionError):nodes.resume()
   self.assertEqual(run.call_count,1)
 def test_owned_paused_node_resumes_before_cleanup(self):
  with tempfile.TemporaryDirectory() as tmp:
   nodes=trial.Nodes(Path(tmp),'network','files');nodes.paused={'owned'}
   with patch.object(trial.st,'run',side_effect=[nodes.session.encode(),b'owned']) as run:nodes.resume()
   self.assertFalse(nodes.paused);self.assertEqual(run.call_args.args[0],['docker','unpause','owned'])
 def test_foreign_container_cannot_be_stopped(self):
  with tempfile.TemporaryDirectory() as tmp:
   nodes=trial.Nodes(Path(tmp),'network','files');nodes.items={'worker1':'foreign'}
   with patch.object(trial.st,'run',return_value=b'other-session') as run,self.assertRaises(AssertionError):nodes.close()
   self.assertEqual(run.call_count,1)
 def test_executor_cleanup_requires_both_workers(self):
  with tempfile.TemporaryDirectory() as tmp:
   nodes=trial.Nodes(Path(tmp),'network','files');nodes.items={'worker1':'one','worker2':'two'}
   with patch.object(trial.st,'run',side_effect=[b'PID CMD\n1 idle',b'PID CMD\n2 CoarseGrainedExecutorBackend']):self.assertFalse(nodes.empty())
   with patch.object(trial.st,'run',side_effect=[b'PID CMD\n1 idle',b'PID CMD\n1 idle']):self.assertTrue(nodes.empty())
if __name__=='__main__':unittest.main()
