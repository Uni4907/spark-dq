import unittest,sys,tempfile,json
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import cluster_headers as h,plot_cluster_headers as plot
class HeaderExperimentTests(unittest.TestCase):
 def test_balanced_paired_plan(self):
  trials=h.plan();self.assertEqual(20,len(trials));self.assertEqual(4,sum(v['warmup'] for v in trials))
  for a,b in zip(trials[4::2],trials[5::2]):
   self.assertEqual({a['variant'],b['variant']},set(h.VARIANTS))
   for key in ('mode','profile','repeat'):self.assertEqual(a[key],b[key])
  self.assertEqual(trials,h.plan())
 def test_output_locations_are_not_semantic_metrics(self):
  a=[dict(ruleId='missing-name',metrics={'blankCount':0},anomalyPath='/baseline/result')]
  b=[dict(ruleId='missing-name',metrics={'blankCount':0},anomalyPath='/candidate/result')]
  self.assertEqual(h.semantic_metrics(a),h.semantic_metrics(b))
  b[0]['metrics']['blankCount']=1
  self.assertNotEqual(h.semantic_metrics(a),h.semantic_metrics(b))
 def test_plot_rejects_tampered_evidence(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory)
   for name,value in [('summary',{'ok':True}),('measurements',[]),('compatibility',[])]:
    (root/(name+'.json')).write_text(json.dumps(value))
   (root/'COMPLETE').write_text(json.dumps({name:plot.digest(root/(name+'.json')) for name in ('summary','measurements','compatibility')}))
   self.assertEqual([],plot.verified(root))
   (root/'measurements.json').write_text('[{}]')
   with self.assertRaises(ValueError):plot.verified(root)
 def test_plot_requires_success(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory)
   for name,value in [('summary',{'ok':False}),('measurements',[]),('compatibility',[])]:
    (root/(name+'.json')).write_text(json.dumps(value))
   (root/'COMPLETE').write_text(json.dumps({name:plot.digest(root/(name+'.json')) for name in ('summary','measurements','compatibility')}))
   with self.assertRaises(ValueError):plot.verified(root)
 def test_invalid_repeats(self):
  for value in (0,4,True,1.5):
   with self.assertRaises(ValueError):h.plan(value)
 def test_summary_preserves_slow_observations(self):
  records=[]
  for mode in ('local','cluster'):
   for profile in ('extended','governance'):
    for variant in h.VARIANTS:
     for wall in (10,100):records.append(dict(mode=mode,profile=profile,variant=variant,warmup=False,containerWallSeconds=wall,driverExecution={'inputReadAndCountMs':wall*1000}))
  groups=h.summarize(records);self.assertEqual(8,len(groups));self.assertTrue(all(v['medianWallSeconds']==55 and v['maxWallSeconds']==100 for v in groups))
  with self.assertRaises(ValueError):h.summarize([])
 def test_signed_negative_fixtures_are_targeted(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);h.fixtures(root)
   for folder in root.iterdir():
    manifest=json.loads((folder/'manifest.json').read_text())
    for entry in manifest['files']:
     observed=h.st.digest(folder/entry['path']);self.assertEqual(entry['size'],observed['size'])
     self.assertEqual(entry['sha256']==observed['sha256'],folder.name!='compat-bad-checksum')
   self.assertEqual(2,len(json.loads((root/'compat-second-part-wrong/manifest.json').read_text())['files']))
if __name__=='__main__':unittest.main()
