import json,tempfile,unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import scale_oracle as scale
from algorithm_oracle import iqr,fd
class ScaleOracleTest(unittest.TestCase):
 def test_disk_oracle_matches_independent_small_reference(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp)/'data';actual=scale.prepare(root,2200);rows=[scale.row(i) for i in range(2200)]
   for name,(metrics,indexes) in {'iqr':iqr([r['n'] for r in rows]),'fd':fd(rows,['x'],['y'])}.items():
    self.assertEqual(actual['metrics'][name],metrics)
    expected=sorted(scale.encode(rows[i]) for i in indexes)
    self.assertEqual((root/(name+'.expected.sorted')).read_text().splitlines(keepends=True),expected)
 def test_external_multiset_preserves_duplicate_count(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);(root/'input').write_text('b\na\na\n');value=scale.sorted_digest(root/'input',root/'sorted');self.assertEqual(value['rows'],3);self.assertEqual((root/'sorted').read_text(),'a\na\nb\n')
if __name__=='__main__':unittest.main()
