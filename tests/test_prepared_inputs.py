import copy,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import prepared_inputs as p
import storage as st

class PreparedInputsTest(unittest.TestCase):
 def test_edge_values_preserve_controlled_csv_boundaries(self):
  rows=p.edge_rows()
  self.assertEqual(len(rows),20);self.assertEqual(rows[0][1],'\\N')
  self.assertEqual(rows[1][1],'');self.assertEqual(rows[2][1],'   ')
  self.assertIn('\n',rows[1][-1]);self.assertIn('\r\n',rows[2][-1])
  self.assertIn('"',rows[3][-1]);self.assertIn('中文',rows[0][-1])
 def test_preparation_cost_and_actual_partitions_remain_visible(self):
  row=dict(rows=100,profile='extended',format='PARQUET',edge=False,wallSeconds=2,
   execution={'inputReadAndCountMs':100,'inputPartitions':2})
  other=copy.deepcopy(row);other['wallSeconds']=4
  edge=copy.deepcopy(row);edge.update(edge=True,wallSeconds=900)
  group=p.summarize([row,other,edge],{'100/PARQUET':{'wallSeconds':12}})[0]
  self.assertEqual(group['medianWallSeconds'],3);self.assertEqual(group['preparePlusOneRunSeconds'],15)
  self.assertEqual(group['actualPartitions'],[2]);self.assertEqual(group['repeats'],2)
 def test_no_preparation_cost_for_original_csv(self):
  row=dict(rows=100,profile='governance',format='CSV_SINGLE',edge=False,wallSeconds=4,
   execution={'inputReadAndCountMs':250,'inputPartitions':1})
  self.assertEqual(p.summarize([row],{})[0]['preparePlusOneRunSeconds'],4)
 def test_anomaly_digest_preserves_null_and_multiplicity(self):
  import json
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);rule=root/'result/anomalies/missing-name';rule.mkdir(parents=True)
   file=rule/'part-000.json';row={name:None for name in p.b.COLUMNS}
   file.write_text(json.dumps(row)+'\n');one=p.anomaly_hash(root)
   file.write_text((json.dumps(row)+'\n')*2);self.assertNotEqual(one,p.anomaly_hash(root))
 def test_owned_timeout_stops_only_its_container(self):
  import subprocess
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp)
   def fake(args,**kwargs):
    if args[:2]==['docker','create']:return b'owned\n'
    if args[:2]==['docker','start']:raise st.StorageError('timeout')
    if args[:2]==['docker','inspect']:return b'session\n' if 'sparkdq.prepared' in args[3] else b'true\n'
    if args[:2]==['docker','stop']:return b'owned\n'
    raise AssertionError(args)
   with patch.object(st,'run',side_effect=fake) as run,patch.object(subprocess,'run') as logs:
    with self.assertRaises(st.StorageError):p.worker(root/'case',root,{},'image','session',timeout=1)
    stops=[call.args[0] for call in run.call_args_list if call.args[0][:2]==['docker','stop']]
    self.assertEqual(stops,[['docker','stop','--time','10','owned']])
    create=run.call_args_list[0].args[0]
    for expected in ('none','--read-only','/tmp:rw,exec,size=768m','1s'):self.assertIn(expected,create)
    self.assertEqual(logs.call_args.args[0],['docker','logs','owned'])
if __name__=='__main__':unittest.main()
