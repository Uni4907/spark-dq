import unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"infrastructure"))
from algorithm_experiments import confusion,effect_data,perf_rows
from algorithm_oracle import fd
class AlgorithmExperimentTest(unittest.TestCase):
 def test_zero_denominators_are_unavailable_not_perfect(self):
  m=confusion(set(),set(),set());self.assertIsNone(m["accuracy"]);self.assertIsNone(m["precision"]);self.assertIsNone(m["recall"]);self.assertIsNone(m["F1"])
 def test_missed_positive_keeps_zero_recall_and_f1(self):
  m=confusion(set(),{"p"},{"p","n"});self.assertEqual((m["TP"],m["FN"],m["TN"]),(0,1,1));self.assertEqual(m["recall"],0);self.assertEqual(m["F1"],0);self.assertIsNone(m["precision"])
 def test_conflict_participation_differs_from_modification_origin(self):
  rows=[{"x":"a","y":"m"},{"x":"a","y":"m"},{"x":"a","y":"n"}];m,ids=fd(rows,["x"],["y"]);self.assertEqual(ids,[0,1,2]);self.assertEqual(m["conflictRows"],3)
  self.assertEqual(confusion({0,1,2},{2},{0,1,2})["precision"],1/3)
 def test_no_eligible_rows_has_explicit_state(self):
  self.assertEqual(fd([{"x":None,"y":" "}],["x"],["y"])[0]["status"],"NO_VALID_ROWS")
 def test_generators_repeat_exactly(self):
  self.assertEqual(effect_data(),effect_data());self.assertEqual(perf_rows(100),perf_rows(100));self.assertEqual(len(effect_data()[3]),18)
if __name__=="__main__":unittest.main()
