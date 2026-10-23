#!/usr/bin/env python3
"""Real Hive table -> controlled product dataset -> detection/report/replay."""
from pathlib import Path
import csv,json,sys,collections,subprocess,time,traceback
P=Path('/home/sparkdq/projects/spark-dq');sys.path[:0]=[str(P/'infrastructure'),str(P/'tests')]
import release_lab as lab
from iqr_acceptance import Client
from algorithm_oracle import iqr,fd
root=Path(sys.argv[1]);access=json.loads((root/'access.json').read_text());c=Client(access['apiUrl']);out=root/('hive-acceptance-'+time.strftime('%Y%m%d-%H%M%S'));out.mkdir();summary=dict(ok=False)
def get(path):
 code,data=c.call(path);assert code==200,(code,data);return data
def multiset(rows):return collections.Counter(json.dumps(row,sort_keys=True,ensure_ascii=False) for row in rows)
with (root/'reference.csv').open(encoding='utf-8',newline='') as f:rows=[{k:None if v=='\\N' else v for k,v in r.items()} for r in csv.DictReader(f)]
def verify(task,name):
 folder=out/name;folder.mkdir();assert task['status']=='SUCCEEDED';lab.save(folder/'task.json',task)
 subprocess.run(['docker','cp',access['backendContainer']+':/data/tasks/'+task['id'],str(folder/'spark')],check=True,timeout=30)
 metrics=[]
 for rule in task['rules']:
  if rule['type']=='IQR':m,idx=iqr([r['n'] for r in rows],'1.5')
  else:m,idx=fd(rows,['x'],['y'])
  assert rule['metrics']==m,(rule['metrics'],m)
  actual=[json.loads(line) for part in (folder/'spark/anomalies'/rule['id']).glob('part-*.json') for line in part.read_text().splitlines()]
  assert multiset(actual)==multiset([rows[i] for i in idx]),(actual,idx)
  metrics.append(dict(type=rule['type'],metrics=m,fullAnomalyRows=len(actual)))
 report=get('/tasks/'+task['id']+'/report');lab.save(folder/'report.json',report);(folder/'report.html').write_bytes(c.raw('/tasks/'+task['id']+'/report.html'));lab.save(folder/'verification.json',dict(ok=True,rules=metrics));return metrics
try:
 public=get('/controlled-files');assert all('path' not in item for item in public);lab.save(out/'public-presets.json',public)
 code,data=c.call('/controlled-files/hive/register',None,'POST');assert code==200,(code,data);lab.save(out/'hive-dataset.json',data);h=data['source']['hive'];assert h['catalog']=='HIVE' and h['provider']=='hive' and data['row_count']==14
 original=c.raw('/datasets/'+data['id']+'/download');assert lab.sha(root/'inputs/hive.csv')==data['sha256'];assert original==(root/'inputs/hive.csv').read_bytes()
 standard=c.upload((root/'reference.csv').read_bytes(),'Hive-CSV-reference.csv');lab.save(out/'csv-dataset.json',standard)
 rules=[dict(type='IQR',columns=['n'],params=dict(k='1.5')),dict(type='FD',columns=['x'],params=dict(dependentColumns=['y']))]
 hive=c.task(dict(datasetId=data['id'],rules=rules));a=verify(hive,'hive-detection')
 reference=c.task(dict(datasetId=standard['id'],rules=rules));b=verify(reference,'csv-detection');assert a==b
 replay=c.task({},'/tasks/'+hive['id']+'/rerun');assert verify(replay,'independent-replay')==a;assert hive['snapshot']==get('/tasks/'+hive['id'])['snapshot'];assert original==c.raw('/datasets/'+data['id']+'/sources/1/download')
 # Transaction release and natural idempotence after completed task work.
 code,same=c.call('/controlled-files/hive/register',None,'POST');assert code==200 and same['id']==data['id']
 summary.update(ok=True,datasetId=data['id'],csvDatasetId=standard['id'],hiveTaskId=hive['id'],csvTaskId=reference['id'],replayTaskId=replay['id'],source=h,engineJarSha256=json.loads((root/'lab.json').read_text())['candidate']['engineJarSha256'],scope='Embedded Derby real Hive SerDe table, admin export with controlled snapshot; not live arbitrary Hive service',fullMetricsAndAnomalyMultisetsEqual=True)
except Exception:summary['failure']=traceback.format_exc();raise
finally:lab.save(out/'summary.json',summary);print(json.dumps(dict(out=str(out),**summary)),flush=True)
