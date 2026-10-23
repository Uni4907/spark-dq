import unittest,sys,tempfile
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import cluster_formats as f
class FormatsTest(unittest.TestCase):
 def test_randomized_complete_balanced_triplets(self):
  values=f.plan([200000],2,9);self.assertEqual(values,f.plan([200000],2,9));self.assertEqual(len(values),26)
  for row in values:
   if row['warmup']:continue
   peers=[v for v in values if not v['warmup'] and (v['rows'],v['profile'],v['repeat'],v['mode'])==(row['rows'],row['profile'],row['repeat'],row['mode'])];self.assertEqual({v['format'] for v in peers},set(f.FORMATS))
 def test_oversized_or_invalid_plan_rejected(self):
  for rows,repeats in [([100,200,300],2),([100,100],1),([],1)]:
   with self.assertRaises(ValueError):f.plan(rows,repeats,1)
 def test_strict_break_even_requires_better_than_tie(self):self.assertEqual(f.break_even(20,10,5),5);self.assertEqual(f.break_even(0,10,5),1)
 def test_slower_or_equal_worker_never_breaks_even(self):self.assertIsNone(f.break_even(2,5,5));self.assertIsNone(f.break_even(2,5,6))
 def test_invalid_timings_not_reported(self):
  for values in [(-1,2,1),(1,float('nan'),1),(1,2,float('inf'))]:
   with self.assertRaises(ValueError):f.break_even(*values)
 def record(self,kind,wall):return {'rows':100,'profile':'extended','mode':'local','format':kind,'warmup':False,'containerWallSeconds':wall,'driverExecution':{'inputReadAndCountMs':1000,'sourcePartitions':1},'resources':{'sampledCpuSecondsLowerBound':1,'sampledAggregateMemoryPeakBytes':1048576}}
 def test_summary_charges_conversion_and_marks_models(self):
  rows=[self.record('CSV_SINGLE',10),self.record('CSV',5),self.record('PARQUET',11)];prep={'100/CSV':{'wallSeconds':20},'100/PARQUET':{'wallSeconds':2}};values=f.summarize(rows,prep);csv=next(v for v in values if v['format']=='CSV');self.assertEqual(csv['preparePlusOneRunSeconds'],25);self.assertEqual(csv['strictBreakEvenRuns'],5);self.assertEqual(csv['modeledSecondsForFiveRuns'],45);self.assertIsNone(values[-1]['strictBreakEvenRuns'])
 def test_unbalanced_formats_refuse_summary(self):
  with self.assertRaises(ValueError):f.summarize([self.record('CSV_SINGLE',1)],{})
 def test_original_single_csv_input_contract_unchanged(self):
  self.assertEqual(f.c.Comparison(Path('/tmp/example'),'test').input_request({'rows':100}),{'input':'/cluster/input/100.csv'})
 def test_pinned_prepared_manifest_cannot_be_silently_changed(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);folder=root/'inputs/100-csv';folder.mkdir(parents=True);(folder/'manifest.json').write_text('{}');cluster=f.Formats(root,'test');cluster.preparations={'100/CSV':{'manifestSha256':'0'*64}}
   with self.assertRaises(AssertionError):cluster.input_request({'rows':100,'format':'CSV'})
 def test_partition_observations_do_not_invent_four_source_partitions(self):
  cluster=f.Formats(Path('/tmp/example'),'test');cluster.preparations={'100/PARQUET':{'manifestSha256':'abc'}};e={'inputFormat':'PARQUET','inputFiles':4,'sourcePartitions':2,'repartitionApplied':True,'inputManifestSha256':'abc'};cluster.check_input({'rows':100,'format':'PARQUET'},e)
  e['repartitionApplied']=False
  with self.assertRaises(AssertionError):cluster.check_input({'rows':100,'format':'PARQUET'},e)
 def test_native_import_activity_refuses_experiment_without_cancelling_user(self):
  with patch.object(f.b,'source_idle'),patch.object(f.rt,'active',return_value=1),self.assertRaises(f.st.StorageError):f.Formats(Path('/tmp/example'),'test').idle()
if __name__=='__main__':unittest.main()
