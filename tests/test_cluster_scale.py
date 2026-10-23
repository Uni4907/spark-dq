import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import cluster_scale as scale
class ScaleTest(unittest.TestCase):
 def records(self):return [{'id':str(i),'sourceRows':rows,'format':kind,'action':action,'durationMs':10000,'inputBytes':14000000,'totalRows':rows if action=='detect' else rows//2} for i,(rows,kind,action) in enumerate((r,k,a) for r in scale.SIZES for k in ('CSV','PARQUET') for a in ('detect','govern','recheck'))]
 def test_size_admission_rejects_unbounded_duplicate_and_boolean(self):
  scale.validate_options([50000,200000],2100)
  for sizes in ([],[True],[50000,50000],[2000000],[50001]):
   with self.assertRaises(ValueError):scale.validate_options(sizes,2100)
  for maximum in (True,599,2401):
   with self.assertRaises(ValueError):scale.validate_options([50000],maximum)
 def test_missing_or_duplicated_trial_cannot_claim_complete_matrix(self):
  records=self.records();self.assertTrue(scale.budget_summary(records,scale.SIZES)['allPersistedDurationsBelowBudget'])
  for invalid in (records[:-1],records+[records[0]],records[:-1]+[records[0]]):
   with self.assertRaises(ValueError):scale.budget_summary(invalid,scale.SIZES)
 def test_negative_margin_is_reported_not_silently_clamped(self):
  records=self.records();records[0]['durationMs']=300100;summary=scale.budget_summary(records,scale.SIZES);self.assertEqual(summary['minimumConservativeMarginMs'],-100);self.assertFalse(summary['allPersistedDurationsBelowBudget'])
 def test_upload_and_row_count_and_duration_evidence(self):
  for field,value in [('inputBytes',20*1024**2+1),('inputBytes',0),('totalRows',49999),('durationMs',-1),('durationMs',True)]:
   records=self.records();records[0][field]=value
   with self.assertRaises(ValueError):scale.budget_summary(records,scale.SIZES)
 def test_reused_task_identity_cannot_count_as_independent_trials(self):
  records=self.records();records[-1]['id']=records[0]['id']
  with self.assertRaises(ValueError):scale.budget_summary(records,scale.SIZES)
if __name__=='__main__':unittest.main()
