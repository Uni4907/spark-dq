import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import header_service_acceptance as service
class CandidateServiceChecks(unittest.TestCase):
 def task(self,status='FAILED',cleanup='CLEAR',metrics=None):return {'status':status,'clusterExecution':{'cleanupObservation':cleanup},'rules':[{'metrics':metrics}]}
 def test_terminal_success_requires_cluster_clear(self):
  with self.assertRaises(AssertionError):service.terminal(self.task('SUCCEEDED','REMOTE_RESOURCES_PRESENT'),'SUCCEEDED')
 def test_interruption_cannot_publish_even_zero_partial_metrics(self):
  with self.assertRaises(AssertionError):service.terminal(self.task(metrics={'blankCount':0}),'FAILED')
 def test_interrupted_task_can_finish_only_with_clear_and_no_results(self):
  task=self.task();self.assertIs(task,service.terminal(task,'FAILED'))
 def test_wrong_terminal_state_is_rejected(self):
  with self.assertRaises(AssertionError):service.terminal(self.task('CANCELLED'),'FAILED')
 def test_worker_order_is_irrelevant_but_all_seven_counts_must_match(self):
  expected=service.b.oracle(100,'extended');rules=service.b.rules('extended')
  task={'rules':[dict(row,metrics=expected['metrics'][row['id']]) for row in rules]}
  result={'results':[{'ruleId':row['id'],'metrics':expected['metrics'][row['id']]} for row in reversed(rules)]}
  service.check_counts(task,result,expected)
  result['results'][0]['metrics']={}
  with self.assertRaises(AssertionError):service.check_counts(task,result,expected)
if __name__=='__main__':unittest.main()
