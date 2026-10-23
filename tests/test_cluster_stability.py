import sys,unittest,tempfile,json
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import cluster_stability as soak,plot_cluster_stability as plot
class StabilityTest(unittest.TestCase):
 def samples(self):return [{'seconds':i,'roles':{'backend':{'cpuUsec':i*1000000,'memoryBytes':(i+1)*100,'oom':0,'oomKill':0}}} for i in (0,1)]
 def test_bounded_parameters(self):
  soak.validate_options(3,10000,900,1800)
  for values in [(0,10000,900,1800),(True,10000,900,1800),(3,10001,900,1800),(3,30000,900,1800),(3,10000,900,900),(3,10000,900,3000)]:
   with self.assertRaises(ValueError):soak.validate_options(*values)
 def test_cache_growth_is_observed_without_false_oom_claim(self):
  result=soak.summarize_resources(self.samples());self.assertEqual(result['sampledAggregateMemoryPeakBytes'],200);self.assertEqual(result['roles']['backend']['sampledCpuSeconds'],1);self.assertEqual(result['roles']['backend']['oomDelta'],0)
 def test_cpu_reset_is_not_silently_accepted(self):
  values=self.samples();values[1]['roles']['backend']['cpuUsec']=-1
  with self.assertRaises(ValueError):soak.summarize_resources(values)
 def test_oom_event_is_failure_even_without_kill(self):
  values=self.samples();values[1]['roles']['backend']['oom']=1
  with self.assertRaises(ValueError):soak.summarize_resources(values)
 def test_missing_resources_and_disappeared_roles_reject(self):
  for values in ([],[{'roles':{}}],[self.samples()[0],{'roles':{}}]):
   with self.assertRaises(ValueError):soak.summarize_resources(values)
 def test_governance_does_not_pretend_invalid_numeric_values_are_fixed(self):
  actual=soak.expected_anomalies(1000,'governance');empty=soak.b.row_digest([])
  for key in ('missing-name','missing-email','duplicate-id','regex-status','type-day'):self.assertEqual(actual[key],empty)
  self.assertNotEqual(actual['range-amount'],empty);self.assertNotEqual(actual['type-amount'],empty)
 def test_duplicate_full_rows_include_group_size_and_null_semantics(self):
  rows=[[None if value=='\\N' else value for value in row]+[2] for row in soak.b.data_rows(4,soak.b.SEED)]
  self.assertEqual(soak.expected_anomalies(4,'extended')['duplicate-id'],soak.b.row_digest(rows))
 def test_disappeared_cgroup_is_failure_not_zero_usage(self):
  with tempfile.TemporaryDirectory() as directory:
   with self.assertRaises(ValueError):soak.sample(Path(directory))
class StabilityPlotTest(unittest.TestCase):
 def fixture(self,root,ok=True):
  values={'summary':{'ok':ok},'records':[],'resource-samples':{'collectorError':None,'samples':[{'roles':{}},{'roles':{}}]}}
  for key,value in values.items():(root/(key+'.json')).write_text(json.dumps(value))
  (root/'COMPLETE').write_text(json.dumps({key:plot.digest(root/(name+'.json')) for key,name in [('summary','summary'),('records','records'),('resources','resource-samples')]}))
 def test_plot_rejects_resource_tampering(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);self.fixture(root);plot.verified(root);(root/'resource-samples.json').write_text('{}')
   with self.assertRaises(ValueError):plot.verified(root)
 def test_plot_rejects_failed_or_incomplete_observation(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);self.fixture(root,False)
   with self.assertRaises(ValueError):plot.verified(root)
if __name__=='__main__':unittest.main()
