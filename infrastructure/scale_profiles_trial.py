#!/usr/bin/env python3
"""Bounded extra distribution probes; single trials are not performance estimates."""
from pathlib import Path
import contextlib,json,os,shutil,sys,time,traceback,uuid
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'infrastructure')]
import scale_oracle as oracle,storage as st
from algorithm_scale_trial import Trial,save,digest
BASE_ROW=oracle.row
def high_cardinality(i):
 value=BASE_ROW(i);value['x']='unique-'+str(i//2);value['y']='same';return value
def hot_high_conflict(i):
 value=BASE_ROW(i);j=i//2;value['x']='hot';value['y']='changed' if j%2 else 'base'
 if j%10<2:value['n']='9007199254840993.0000000001'
 return value
def main():
 assert not st.run(['docker','ps','-q']).strip(),'Other experiment still running'
 root=ROOT/'.runtime'/('t4-profiles-'+time.strftime('%Y%m%d-%H%M%S'));root.mkdir(mode=0o700);summary=dict(ok=False,scope='Distribution stress probes: one trial per profile/mode, not a repeated performance experiment',profiles=[]);deadline=time.monotonic()+1000
 try:
  for profile,function in [('high_cardinality',high_cardinality),('hot_high_conflict',hot_high_conflict)]:
   assert shutil.disk_usage(root).free>=8*1024**3 and shutil.disk_usage('/mnt/c').free>=8*1024**3
   available=int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')))*1024;assert available>=6*1024**3
   folder=root/profile;folder.mkdir();(folder/'inputs').mkdir();(folder/'output').mkdir();os.chown(folder/'output',185,0);shutil.copyfile(ROOT/'spark-engine/target/spark-engine-0.1.0.jar',folder/'app.jar');(folder/'app.jar').chmod(0o444);oracle.row=function;expected=oracle.prepare(folder/'oracle',200000);shutil.copyfile(folder/'oracle/input.csv',folder/'inputs/200000.csv');cluster=Trial(folder,uuid.uuid4().hex);records=[]
   try:
    cluster.start_cluster()
    for mode in ('local','cluster'):records.append(cluster.measure(200000,0,mode,expected,deadline,False));print(profile,mode,'verified',flush=True)
   finally:cluster.close()
   record=dict(profile=profile,rows=200000,inputBytes=expected['inputBytes'],inputSha256=digest(folder/'inputs/200000.csv'),metrics=expected['metrics'],bothModesFullMultisetsVerified=True,records=records);save(folder/'summary.json',record);summary['profiles'].append(record)
  summary['ok']=True
 except Exception:summary['failure']=traceback.format_exc();(root/'failure.txt').write_text(summary['failure'])
 finally:oracle.row=BASE_ROW;save(root/'summary.json',summary);print(json.dumps(dict(root=str(root),ok=summary['ok'],profiles=[x['profile'] for x in summary['profiles']])),flush=True)
if __name__=='__main__':main()
