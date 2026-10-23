import tempfile,unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import scale_oracle as disk,scale_profiles_trial as profiles,b3_baseline_refresh as baseline
from algorithm_oracle import iqr,fd
import algorithm_experiments as historical
class FollowupOracleTest(unittest.TestCase):
 def verify(self,function,columns,encoder):
  old=(disk.row,disk.COLUMNS,disk.encode)
  try:
   disk.row=function;disk.COLUMNS=columns;disk.encode=encoder
   with tempfile.TemporaryDirectory() as temp:
    root=Path(temp)/'data';actual=disk.prepare(root,2200);rows=[function(i) for i in range(2200)]
    for name,(metrics,indexes) in {'iqr':iqr([r['n'] for r in rows]),'fd':fd(rows,['x'],['y'])}.items():
     self.assertEqual(actual['metrics'][name],metrics);self.assertEqual((root/(name+'.expected.sorted')).read_text().splitlines(keepends=True),sorted(encoder(rows[i]) for i in indexes))
    return actual
  finally:disk.row,disk.COLUMNS,disk.encode=old
 def test_high_cardinality_profile_matches_independent_reference(self):
  actual=self.verify(profiles.high_cardinality,disk.COLUMNS,disk.encode);self.assertEqual(actual['metrics']['fd']['conflictRows'],0)
 def test_hot_high_conflict_profile_matches_independent_reference(self):
  actual=self.verify(profiles.hot_high_conflict,disk.COLUMNS,disk.encode);self.assertEqual(actual['metrics']['fd']['conflictRows'],2200)
 def test_historical_adapter_preserves_original_generator_and_reference(self):
  self.assertEqual([{k:v for k,v in baseline.row(i).items() if k!='n'} for i in range(2200)],historical.perf_rows(2200));self.verify(baseline.row,baseline.COLUMNS,baseline.encode)
if __name__=='__main__':unittest.main()
