import sys,unittest,tempfile
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import cluster_lab as lab,cluster_timeout as timeout
class TimeoutTest(unittest.TestCase):
 def fixture(self,root):
  p=root/'123';p.mkdir();(p/'cmdline').write_bytes(b'java\0cn.sparkdq.engine.Engine\0/data/tasks/owned/request.json\0');fields=['S','45']+['0']*17+['987'];(p/'stat').write_text('123 (java process) '+' '.join(fields));(p/'cgroup').write_text('0::/docker/owned-container');return timeout.process_identity(123,'owned-container','cn.sparkdq.engine.Engine','/data/tasks/owned/request.json',45,root)
 def test_pid_reuse_never_matches_original_signal_target(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);identity=self.fixture(root);self.assertTrue(timeout.same_process(identity,root));path=root/'123/stat';path.write_text(path.read_text().replace('987','988'));self.assertFalse(timeout.same_process(identity,root))
 def test_wrong_task_class_parent_or_container_is_not_signalable(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);identity=self.fixture(root)
   for key,value in [('request','/data/tasks/other/request.json'),('main','cn.sparkdq.engine.PreparedInput'),('parent',46),('containerId','foreign-container')]:
    self.assertFalse(timeout.same_process(dict(identity,**{key:value}),root))
 def test_exited_process_is_not_resumed(self):self.assertFalse(timeout.same_process({'pid':-1,'startTicks':'1','containerId':'x','main':'x','request':'x','parent':1}))
 def test_lab_budget_cannot_exceed_business_max_or_use_boolean(self):
  self.assertEqual(lab.task_budget(5),5);self.assertEqual(lab.task_budget(300),300)
  for value in (True,False,4,301,'60',60.0):
   with self.assertRaises(ValueError):lab.task_budget(value)
 def test_invalid_budget_rejects_before_creating_files_or_docker(self):
  with patch.object(lab.st,'run') as run,self.assertRaises(ValueError):lab.create(Path('/nonexistent'),'unused',task_timeout_seconds=301)
  run.assert_not_called()
 def test_budget_template_fails_closed_on_ambiguity(self):
  key='      SPARK_DQ_EXECUTION: standalone';self.assertIn('SPARK_DQ_TASK_TIMEOUT_SECONDS: "60"',lab.configure_budget(key,60))
  for value in ('',key+'\n'+key,key+'\n SPARK_DQ_TASK_TIMEOUT_SECONDS: 300'):
   with self.assertRaises(ValueError):lab.configure_budget(value,60)
if __name__=='__main__':unittest.main()
