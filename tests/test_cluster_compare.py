import unittest,sys,tempfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import cluster_compare as c
class CompareTest(unittest.TestCase):
 def test_null_ipam_network_does_not_break_readonly_inventory(self):
  from unittest.mock import patch
  import json
  import tempfile
  with tempfile.TemporaryDirectory() as temp:
   cluster=c.Comparison(Path(temp),'test-session');cluster.workers=[]
   with patch.object(c.st,'run',side_effect=[b'bridge host none',json.dumps([{'IPAM':{'Config':None}},{'IPAM':{'Config':[{'Subnet':'10.0.0.0/8'}]}}]).encode(),b'[]',RuntimeError('stop before mutation')]) as run,self.assertRaisesRegex(RuntimeError,'stop before mutation'):cluster.start_cluster()
   self.assertIn('192.168.240.0/28',run.call_args.args[0])
 def test_retained_old_lab_ranges_do_not_require_network_deletion(self):
  self.assertEqual(c.choose_subnet(['10.240.240.0/24','192.168.240.0/24','10.241.240.0/24']),'10.242.240.0/28')
 def test_subnet_avoids_routes_and_existing_docker_ranges(self):
  self.assertEqual(c.choose_subnet(['10.0.0.0/8','default','192.168.240.0/28']),'192.168.240.16/28')
  with self.assertRaises(ValueError):c.choose_subnet(['10.0.0.0/8','192.168.0.0/16'])
 def test_pairs_balanced_and_deterministic(self):
  trials=c.plan([10000,200000],2,7);self.assertEqual(trials,c.plan([10000,200000],2,7));self.assertEqual(len(trials),18)
  for row in trials:
   peer=[v for v in trials if (v['rows'],v['profile'],v['repeat'],v['warmup'])==(row['rows'],row['profile'],row['repeat'],row['warmup'])];self.assertEqual({v['mode'] for v in peer},{'local','cluster'})
 def test_invalid_plan(self):
  for rows,repeats in [([],2),([100,100],1),([99],2),([250001],2),([100],0),([100,200,300,400],3)]:
   with self.assertRaises(ValueError):c.plan(rows,repeats,1)
 def test_resource_sum_uses_simultaneous_memory_not_sum_of_individual_peaks(self):
  samples=[{'roles':{'driver':{'cpuUsec':10,'memoryBytes':80},'worker':{'cpuUsec':110,'memoryBytes':20}}},{'roles':{'driver':{'cpuUsec':30,'memoryBytes':20},'worker':{'cpuUsec':140,'memoryBytes':80}}}]
  value=c.resources(samples,{'worker':100},['driver','worker']);self.assertEqual(value['sampledAggregateMemoryPeakBytes'],100);self.assertAlmostEqual(value['sampledCpuSecondsLowerBound'],.00007)
 def test_exit_missing_samples_preserve_last_known_cpu(self):
  value=c.resources([{'roles':{'driver':{'cpuUsec':30,'memoryBytes':50}}},{'roles':{}}],{},['driver']);self.assertEqual(value['sampledCpuSecondsLowerBound'],.00003)
 def test_missing_role_or_counter_reset_is_not_zero_usage(self):
  for samples in [[],[{'roles':{}}],[{'roles':{'driver':{'cpuUsec':3,'memoryBytes':2}}},{'roles':{'driver':{'cpuUsec':1,'memoryBytes':2}}}]]:
   with self.assertRaises(ValueError):c.resources(samples,{},['driver'])
 def test_removed_cgroup_is_unavailable(self):
  with tempfile.TemporaryDirectory() as folder:self.assertIsNone(c.read_cgroup(Path(folder)))
 def test_aggregate_excludes_warmup(self):
  records=[]
  for mode in ('local','cluster'):
   for warm,seconds in ((True,999),(False,10),(False,20)):
    records.append({'rows':100,'profile':'extended','mode':mode,'warmup':warm,'containerWallSeconds':seconds,'resources':{'sampledCpuSecondsLowerBound':1,'sampledAggregateMemoryPeakBytes':1048576}})
  result=c.aggregate(records);self.assertEqual(len(result),2);self.assertTrue(all(v['n']==2 and v['medianWallSeconds']==15 for v in result))
 def test_incomplete_mode_not_reported(self):
  with self.assertRaises(ValueError):c.aggregate([{'rows':100,'profile':'extended','mode':'local','warmup':False,'containerWallSeconds':1,'resources':{'sampledCpuSecondsLowerBound':1,'sampledAggregateMemoryPeakBytes':1}}])
class PlotIntegrityTest(unittest.TestCase):
 def fixture(self,root):
  import json
  for name,value in [('summary.json',{'ok':True,'measuredTrials':2}),('measurements.json',[])]: (root/name).write_text(json.dumps(value))
  (root/'COMPLETE').write_text(json.dumps({'summary':c.st.digest(root/'summary.json'),'measurements':c.st.digest(root/'measurements.json')}))
 def test_missing_complete_cannot_be_plotted(self):
  import plot_cluster_compare as plot
  with tempfile.TemporaryDirectory() as folder,self.assertRaises(FileNotFoundError):plot.verified(Path(folder))
 def test_changed_measurements_cannot_be_plotted(self):
  import plot_cluster_compare as plot
  with tempfile.TemporaryDirectory() as folder:
   root=Path(folder);self.fixture(root);(root/'measurements.json').write_text('[1]')
   with self.assertRaises(AssertionError):plot.verified(root)
if __name__=='__main__':unittest.main()
