#!/usr/bin/env python3
"""Recheck the exact historical 200k CSV with the current JAR and bounded oracle."""
import json,shutil,sys,time,uuid,statistics,random,contextlib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'infrastructure'),str(ROOT/'tests')]
import scale_oracle as disk,algorithm_experiments as experiment
COLUMNS=['row_id','value','x','y']
def row(i):
 group=i//10;value='1000' if i%1000==0 else str(i%101)
 if i%997==0:value=None
 elif i%991==0:value='bad'
 return dict(row_id='p'+str(i),value=value,x=None if i%1999==0 else 'g'+str(group),y='changed' if group%17==0 and i%10==9 else 'v'+str(group%7),n=value)
def encode(value):return json.dumps({c:value[c] for c in COLUMNS},ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n'
def main():
 assert not experiment.command(['docker','ps','-q']).strip(),'Wait for other experiment to stop'
 root=ROOT/'.runtime'/('t4-b3-baseline-'+time.strftime('%Y%m%d-%H%M%S'));root.mkdir(mode=0o755);(root/'inputs').mkdir();(root/'trials').mkdir();assert shutil.disk_usage(root).free>=8*1024**3
 original=(disk.COLUMNS,disk.row,disk.encode)
 try:
  disk.COLUMNS=COLUMNS;disk.row=row;disk.encode=encode;expected=disk.prepare(root/'oracle',200000)
 finally:disk.COLUMNS,disk.row,disk.encode=original
 shutil.copyfile(root/'oracle/input.csv',root/'inputs/200000.csv');historic=ROOT/'.runtime/b3-algorithms-20261021-final/inputs/200000.csv';assert historic.is_file();assert experiment.file_sha(historic)==experiment.file_sha(root/'inputs/200000.csv'),'Historical CSV bytes differ'
 candidate=json.loads((ROOT/'.runtime/t1-build-20261021-165747/candidate.json').read_text());image=candidate['backendImageId'];jar=experiment.command(['docker','run','--rm','--network','none','--entrypoint','sha256sum',image,'/app/engine/spark-engine-0.1.0.jar']).decode().split()[0];assert jar==candidate['engineJarSha256']
 experiment.save(root/'environment.json',dict(historicalCsv=str(historic),sameCsvSha256=experiment.file_sha(historic),inputBytes=expected['inputBytes'],engineJarSha256=jar,imageId=image,cpuQuota=2,memoryBytes=2*1024**3,heapMiB=1024,inputPartitions=4,shufflePartitions=2,scope='Exact B3 200k input and single-rule protocol, current candidate JAR; historical JAR differs; no result pooling',oraclePolicy=expected['memoryPolicy']))
 def verify(folder,rows,rules,unused):
  result=json.loads((folder/'result/result.json').read_text());assert result['totalRows']==200000 and result['sparkVersion']=='3.5.8';rid=rules[0]['id'];assert len(result['results'])==1 and result['results'][0]['metrics']==expected['metrics'][rid];total=0
  raw=folder/'actual.unsorted'
  with raw.open('w') as output:
   for part in (folder/'result/anomalies'/rid).glob('part-*.json'):
    total+=part.stat().st_size
    with part.open() as stream:
     for line in stream:
      value=json.loads(line);assert set(value)==set(COLUMNS);output.write(encode(value))
  actual=disk.sorted_digest(raw,folder/'actual.sorted');assert actual==expected['anomalies'][rid];return result,{},total
 experiment.verify=verify;plan=[dict(kind=k,warmup=True,repeat=0) for k in ('IQR','FD')];formal=[dict(kind=k,warmup=False,repeat=i) for k in ('IQR','FD') for i in (1,2,3)];random.Random(20261021+200000).shuffle(formal);plan+=formal;records=[];summary=dict(ok=False)
 try:
  for i,item in enumerate(plan):
   record,_=experiment.trial(root,i,'200000.csv',range(200000),[experiment.rule(item['kind'])],None,image,uuid.uuid4().hex,time.monotonic()+300,item);records.append(record);experiment.save(root/('record-'+str(i)+'.json'),record);print(json.dumps(dict(algorithm=item['kind'],warmup=item['warmup'],seconds=record['containerWallSeconds'])),flush=True)
  groups=[]
  for kind in ('IQR','FD'):
   values=[r for r in records if r['kind']==kind and not r['warmup']];times=[r['containerWallSeconds'] for r in values];groups.append(dict(algorithm=kind,formalMeasurements=3,warmups=1,medianWallSeconds=statistics.median(times),minWallSeconds=min(times),maxWallSeconds=max(times)))
  summary.update(ok=True,rows=200000,inputBytes=expected['inputBytes'],sameHistoricalCsv=True,fullAnomalyMultisetsVerified=True,groups=groups)
 finally:experiment.save(root/'summary.json',summary);print(json.dumps(dict(root=str(root),**summary)),flush=True)
if __name__=='__main__':main()
