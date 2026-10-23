import sys,tempfile,unittest,hashlib,copy
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import benchmark as b
import storage as st
class BenchmarkTest(unittest.TestCase):
 def test_fixed_seed_and_no_overwrite(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp)
   first=b.generate(root/'a.csv',20,42);second=b.generate(root/'b.csv',20,42)
   third=b.generate(root/'c.csv',20,43)
   self.assertEqual(first['sha256'],second['sha256']);self.assertNotEqual(first['sha256'],third['sha256'])
   with self.assertRaises(st.StorageError):b.generate(root/'a.csv',20,42)
   self.assertEqual(hashlib.sha256((root/'a.csv').read_bytes()).hexdigest(),first['sha256'])
 def test_known_missing_and_duplicate_counts(self):
  value=b.oracle(6,'basic')
  self.assertEqual(value['totalRows'],6)
  self.assertEqual(value['metrics']['missing-name'],dict(nullCount=1,emptyCount=1,whitespaceCount=1,blankCount=3))
  self.assertEqual(value['metrics']['missing-email']['blankCount'],1)
  self.assertEqual(value['metrics']['duplicate-id'],dict(duplicateGroups=3,duplicateRows=6,excessRows=3))
 def test_extended_and_governance_oracles(self):
  extended=b.oracle(6,'extended');governed=b.oracle(6,'governance')
  self.assertEqual(extended['metrics']['type-amount']['invalidCount'],1)
  self.assertEqual(extended['metrics']['range-amount']['invalidCount'],6)
  self.assertEqual(extended['metrics']['regex-status']['invalidCount'],1)
  self.assertEqual(governed['totalRows'],3);self.assertEqual(governed['metrics']['duplicate-id']['duplicateRows'],0)
  self.assertEqual(governed['metrics']['range-amount']['invalidCount'],3)
  self.assertEqual(governed['audit']['deduplicatedRows'],3)
  self.assertEqual([row[4] for row in b.governed_rows(6,42)],['10']*3)
 def test_odd_tail_and_multiset_digest(self):
  self.assertEqual(b.oracle(5,'governance')['totalRows'],3)
  self.assertEqual([row[4] for row in b.governed_rows(5,42)],['10','10','9'])
  self.assertEqual(b.row_digest([['a'],['b']]),b.row_digest([['b'],['a']]))
  self.assertNotEqual(b.row_digest([['a'],['a']]),b.row_digest([['a']]))
 def test_reproducible_plan_and_warmup_exclusion(self):
  a=b.plan([10000,50000,200000],[1,2,4],2,1,42)
  self.assertEqual(len(a),57);self.assertEqual(sum(x['warmup'] for x in a),3)
  self.assertEqual(a,b.plan([10000,50000,200000],[1,2,4],2,1,42))
  self.assertNotEqual(a,b.plan([10000,50000,200000],[1,2,4],2,1,43))
 def test_plan_bounds(self):
  for rows,parts,repeats,warmups in [([0],[1],2,1),([300000],[1],2,1),([100,100],[1],2,1),([100],[0],2,1),([100],[1,1],2,1),([100],[1],0,1),([100],[1],2,3)]:
   with self.assertRaises(ValueError):b.plan(rows,parts,repeats,warmups,42)
 def test_aggregate_uses_only_verified_measured_trials(self):
  row=dict(rows=100,partitions=1,profile='basic',warmup=False,status='verified',containerWallSeconds=4,execution={'workerCpuMs':2000,'peakRssBytes':100*1024**2,'inputReadAndCountMs':1000,'rulesAndAnomalyWriteMs':2000})
  other=copy.deepcopy(row);other['containerWallSeconds']=6
  warm=copy.deepcopy(row);warm.update(warmup=True,containerWallSeconds=1000)
  fail=copy.deepcopy(row);fail.update(status='failed',containerWallSeconds=2000)
  result=b.aggregate([row,other,warm,fail])[0]
  self.assertEqual(result['repeats'],2);self.assertEqual(result['medianWallSeconds'],5);self.assertEqual(result['inputRowsPerSecond'],20)
 def test_timeout_stops_only_label_verified_owned_worker(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);(root/'cases').mkdir();(root/'datasets').mkdir()
   item=dict(index=1,rows=100,profile='basic',partitions=1,repeat=1,warmup=False)
   def fake(args,**kwargs):
    if args[:2]==['docker','create']:return b'owned-container\n'
    if args[:2]==['docker','start']:raise st.StorageError('timed out')
    if args[:2]==['docker','inspect']:
     return b'session\n' if 'sparkdq.benchmark' in args[3] else b'true\n'
    if args[:2]==['docker','stop']:return b'owned-container\n'
    raise AssertionError(args)
   with patch.object(st,'run',side_effect=fake) as run:
    with self.assertRaises(st.StorageError):b.trial(root,item,'image','session',{},timeout=20)
    stops=[call.args[0] for call in run.call_args_list if call.args[0][:2]==['docker','stop']]
    self.assertEqual(stops,[['docker','stop','--time','10','owned-container']])
    create=run.call_args_list[0].args[0]
    self.assertIn('--read-only',create);self.assertIn('--network',create);self.assertIn('20s',create);self.assertIn('none',create)
if __name__=='__main__':unittest.main()
