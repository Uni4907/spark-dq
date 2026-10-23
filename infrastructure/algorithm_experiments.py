#!/usr/bin/env python3
"""Bounded local Spark correctness/effect/performance experiments, no primary mounts."""
import argparse,collections,csv,hashlib,io,json,os,platform,random,shutil,statistics,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'infrastructure')]
from algorithm_oracle import iqr,fd,DOMAIN,blank
from iqr_acceptance import csv_bytes,digest
from benchmark import MODULES
from cluster_compare import cgroup,read_cgroup
def save(path,obj):
 with path.open('x') as f:json.dump(obj,f,ensure_ascii=False,indent=2)
def command(args,timeout=30):return subprocess.check_output(args,timeout=timeout)
def file_sha(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
 return h.hexdigest()
def primary_fingerprints():
 result={}
 for name in ('spark-dq_mysql-data','spark-dq_files'):
  mount=Path(command(['docker','volume','inspect','--format','{{.Mountpoint}}',name]).decode().strip());assert mount.is_absolute() and mount.is_dir()
  records=[]
  for p in sorted(mount.rglob('*')):
   if p.is_symlink():
    records.append([str(p.relative_to(mount)),0,'symlink:'+digest(os.readlink(p).encode())]);continue
   if p.is_file():records.append([str(p.relative_to(mount)),p.stat().st_size,file_sha(p)])
  result[name]={'files':len(records),'bytes':sum(v[1] for v in records),'treeSha256':digest(json.dumps(records,ensure_ascii=False).encode())}
 return result
def rule(kind,column=None):return {'id':kind.lower(),'type':kind,'columns':[column or ('value' if kind=='IQR' else 'x')],'params':{'k':'1.5'} if kind=='IQR' else {'dependentColumns':['y']},'definitionVersion':1}
def expected(rows,rules):
 return {r['id']:(iqr([row[r['columns'][0]] for row in rows],r['params']['k']) if r['type']=='IQR' else fd(rows,r['columns'],r['params']['dependentColumns'])) for r in rules}
def verify(folder,rows,rules,oracle):
 result=json.loads((folder/'result/result.json').read_text());assert result['totalRows']==len(rows) and result['sparkVersion']=='3.5.8'
 by_id={row['row_id']:row for row in rows};assert len(by_id)==len(rows)
 predictions={};bytes_count=0
 for value in result['results']:
  metrics,indexes=oracle[value['ruleId']];assert value['metrics']==metrics,(value['ruleId'],value['metrics'],metrics)
  records=[]
  for part in (folder/'result/anomalies'/value['ruleId']).glob('part-*.json'):
   bytes_count+=part.stat().st_size
   for line in part.read_text().splitlines():
    item=json.loads(line);assert item==by_id[item['row_id']];records.append(item['row_id'])
  target=[rows[i]['row_id'] for i in indexes];assert collections.Counter(records)==collections.Counter(target),(value['ruleId'],len(records),len(target))
  predictions[value['ruleId']]=set(records)
 return result,predictions,bytes_count
def trial(root,index,input_name,rows,rules,oracle,image,session,deadline,meta):
 folder=root/'trials'/str(index);folder.mkdir();os.chown(folder,1000,1000)
 payload={'taskId':'algorithm-'+session[:8]+'-'+str(index),'input':'/experiment/inputs/'+input_name,'output':'/experiment/output/result','semanticsVersion':'csv-v2','rules':rules}
 save(folder/'request.json',payload)
 name='sparkdq-algexp-'+session[:8]+'-'+str(index)
 args=['docker','create','--name',name,'--label','sparkdq.algorithm.experiment='+session,'--network','none','--read-only','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','512','--cap-drop','ALL','--security-opt','no-new-privileges:true','--tmpfs','/tmp:rw,size=768m','--mount','type=bind,src='+str(root/'inputs')+',dst=/experiment/inputs,readonly','--mount','type=bind,src='+str(folder)+',dst=/experiment/output','-e','SPARK_LOCAL_IP=127.0.0.1','-e','SPARK_MASTER=local[2]','-e','SPARK_INPUT_PARTITIONS=4','-e','SPARK_SHUFFLE_PARTITIONS=2','--entrypoint','timeout',image,'--signal=TERM','--kill-after=10s','300s','java','-Xmx1g']
 args+=['--add-opens=java.base/'+module+'=ALL-UNNAMED' for module in MODULES]
 args+=['-cp','/app/engine/spark-engine-0.1.0.jar:/app/engine/dependency/*','cn.sparkdq.engine.Engine','/experiment/output/request.json']
 cid=command(args,60).decode().strip();save(folder/'created.json',dict(container=cid,**meta));samples=[];waiter=None;state={};started=time.monotonic()
 try:
  command(['docker','start',cid]);cg=cgroup(cid);waiter=subprocess.Popen(['docker','wait',cid],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  while True:
   value=read_cgroup(cg);samples.append({'seconds':time.monotonic()-started,'resource':value})
   if waiter.poll() is not None:break
   if time.monotonic()-started>310 or time.monotonic()>deadline:raise TimeoutError('Bounded experiment deadline')
   time.sleep(.25)
  out,err=waiter.communicate(timeout=3);wall=time.monotonic()-started
  state=json.loads(command(['docker','inspect','--format','{{json .State}}',cid]));assert waiter.returncode==0 and out.strip()==b'0' and state['ExitCode']==0 and not state['OOMKilled'],(out,err,state)
  result,predictions,output_bytes=verify(folder,rows,rules,oracle)
  valid=[v['resource'] for v in samples if v['resource'] is not None]
  record=dict(meta,index=index,status='verified',container=cid,containerWallSeconds=wall,execution=result['execution'],ruleTimings=[{k:v for k,v in item.items() if k in ('ruleId','algorithmMs','anomalyWriteMs')} for item in result['results']],anomalyOutputBytes=output_bytes,sampledCpuSecondsLowerBound=max(v['cpuUsec'] for v in valid)/1e6 if valid else None,sampledMemoryPeakBytes=max(v['memoryBytes'] for v in valid) if valid else None,exitCode=state['ExitCode'],oomKilled=state['OOMKilled'])
  save(folder/'measurement.json',record);return record,predictions
 except BaseException as error:
  save(folder/'FAILED.json',{'error':str(error),'state':state,'elapsedSeconds':time.monotonic()-started});raise
 finally:
  save(folder/'resource-samples.json',samples)
  assert command(['docker','inspect','--format','{{index .Config.Labels "sparkdq.algorithm.experiment"}}',cid]).decode().strip()==session
  if command(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':command(['docker','stop','--time','5',cid],20)
  if waiter and waiter.poll() is None:waiter.kill();waiter.communicate(timeout=5)
  with (folder/'engine.log').open('xb') as f:subprocess.run(['docker','logs',cid],stdout=f,stderr=subprocess.STDOUT,timeout=30,check=True)
def confusion(predicted,positive,eligible):
 predicted=set(predicted)&eligible;positive=set(positive)&eligible
 tp=len(predicted&positive);fp=len(predicted-positive);fn=len(positive-predicted);tn=len(eligible-predicted-positive)
 return {'TP':tp,'FP':fp,'FN':fn,'TN':tn,'eligible':len(eligible),'accuracy':(tp+tn)/len(eligible) if eligible else None,'precision':tp/(tp+fp) if tp+fp else None,'recall':tp/(tp+fn) if tp+fn else None,'F1':2*tp/(2*tp+fp+fn) if 2*tp+fp+fn else None}
def effect_data():
 rows=[{'row_id':'e'+str(i)} for i in range(1000)];clean=[dict(row) for row in rows];rules=[];labels=[]
 for seed in (20261021,20261022,20261023):
  for scenario in ('symmetric','skewed','constant','contamination30','subtle'):
   rng=random.Random(str(seed)+scenario);name=scenario+'_'+str(seed);ratio=.30 if scenario=='contamination30' else .01
   base=[7. if scenario=='constant' else rng.lognormvariate(0,1.4) if scenario=='skewed' else rng.uniform(-1,1) if scenario=='contamination30' else rng.gauss(0,1) for _ in rows]
   corrupt=set(rng.sample(range(4,len(rows)),round(len(rows)*ratio)));positives=[]
   for i,row in enumerate(rows):
    clean[i][name]=format(base[i],'.6f');value=base[i]
    if i in corrupt:value=.25 if scenario=='subtle' else 20 if scenario in ('constant','contamination30') else 40 if scenario=='skewed' else (7 if i%2 else -7);positives.append(row['row_id'])
    row[name]=[None,'','   ','1\n'][i] if i<4 else format(value,'.6f')
   rid='iqr-'+name;rules.append(dict(rule('IQR',name),id=rid));labels.append({'ruleId':rid,'algorithm':'IQR','seed':seed,'scenario':scenario,'injectionRatio':ratio,'positiveIds':positives,'labelMeaning':'synthetically modified numeric rows, not oracle statistical outliers'})
  rng=random.Random(seed);x='fx_'+str(seed);y='fy_'+str(seed);groups=set(rng.sample(range(100),10));modified=[];participants=[]
  for i,row in enumerate(rows):
   key='g|'+str(i//10);base='v'+str((i//10)%7);clean[i][x]=key;clean[i][y]=base
   changed=i//10 in groups and i%10==9;row[x]=None if i in (0,1) else key;row[y]='modified' if changed else base
   if changed:modified.append(row['row_id'])
   if i//10 in groups and i not in (0,1):participants.append(row['row_id'])
  rid='fd-'+str(seed);rules.append(dict(rule('FD'),id=rid,columns=[x],params={'dependentColumns':[y]}));labels.append({'ruleId':rid,'algorithm':'FD','seed':seed,'scenario':'group_participation','positiveIds':participants,'modifiedIds':modified,'labelMeaning':'all eligible members of injected conflicting groups; modification origin separately evaluated'})
 return rows,clean,rules,labels
def perf_rows(n):
 rows=[]
 for i in range(n):
  group=i//10;value='1000' if i%1000==0 else str(i%101)
  if i%997==0:value=None
  elif i%991==0:value='bad'
  rows.append({'row_id':'p'+str(i),'value':value,'x':None if i%1999==0 else 'g'+str(group),'y':'changed' if group%17==0 and i%10==9 else 'v'+str(group%7)})
 return rows
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--image',required=True);p.add_argument('--host-capacity',type=Path,required=True);p.add_argument('--max-seconds',type=int,default=2400);a=p.parse_args()
 root=a.output.absolute();root.resolve().relative_to((ROOT/'.runtime').resolve());assert not root.exists() and 300<=a.max_seconds<=3600
 host=json.loads(a.host_capacity.read_text(encoding='utf-8-sig'));assert host['known'] and host['freeBytes']>=8*1024**3 and shutil.disk_usage(ROOT).free>=8*1024**3
 assert not command(['docker','ps','-q']).strip(),'Other workloads running; benchmark refused'
 root.mkdir(mode=0o755);(root/'inputs').mkdir();(root/'trials').mkdir();deadline=time.monotonic()+a.max_seconds;session=uuid.uuid4().hex
 image=command(['docker','image','inspect','--format','{{.Id}}',a.image]).decode().strip();sources={str(p.relative_to(ROOT)):file_sha(p) for p in [Path(__file__),ROOT/'tests/algorithm_oracle.py',ROOT/'tests/iqr_acceptance.py',ROOT/'spark-engine/target/spark-engine-0.1.0.jar']}
 before=primary_fingerprints();save(root/'primary-before.json',before)
 env={'sourceCommit':command(['git','-c','safe.directory='+str(ROOT),'rev-parse','HEAD']).decode().strip(),'sourceSha256':sources,'imageId':image,'engineJarSha256':command(['docker','run','--rm','--network','none','--entrypoint','sha256sum',image,'/app/engine/spark-engine-0.1.0.jar']).decode().split()[0],'kernel':platform.platform(),'cpuQuota':2,'memoryBytes':2*1024**3,'jvmHeap':'1g','master':'local[2]','inputPartitions':4,'shufflePartitions':2,'sampleIntervalSeconds':.25,'hostCapacity':host,'seedList':[20261021,20261022,20261023],'maxSeconds':a.max_seconds};assert env['engineJarSha256']==env['sourceSha256']['spark-engine/target/spark-engine-0.1.0.jar'],'Image/local JAR mismatch';save(root/'environment.json',env)
 rows,clean,rules,labels=effect_data();(root/'inputs/effect.csv').write_bytes(csv_bytes(list(rows[0]),rows));(root/'inputs/effect-clean.csv').write_bytes(csv_bytes(list(clean[0]),clean));save(root/'effect-labels.json',labels)
 oracle=expected(rows,rules);save(root/'effect-oracle.json',{k:{'metrics':v[0],'anomalyIds':[rows[i]['row_id'] for i in v[1]]} for k,v in oracle.items()})
 record,pred=trial(root,0,'effect.csv',rows,rules,oracle,image,session,deadline,{'kind':'effect','rows':1000,'warmup':False});effects=[]
 for label,r in zip(labels,rules):
  eligible={v['row_id'] for v in rows if (v[r['columns'][0]] is not None and DOMAIN.fullmatch(v[r['columns'][0]]))} if r['type']=='IQR' else {v['row_id'] for v in rows if not any(blank(v[c]) for c in r['columns']+r['params']['dependentColumns'])}
  row={k:v for k,v in label.items() if k not in ('positiveIds','modifiedIds')};row.update(confusion(pred[r['id']],label['positiveIds'],eligible));row['skipped']=len(rows)-len(eligible)
  if r['type']=='FD':row['modifiedRowLabelsAlternative']=confusion(pred[r['id']],label['modifiedIds'],eligible)
  effects.append(row)
 save(root/'effect-results.json',effects);records=[record]
 datasets={};expected_perf={};data={}
 for n in (10000,50000,200000):
  values=perf_rows(n);data[n]=values;raw=csv_bytes(list(values[0]),values);assert len(raw)<=20*1024**2;(root/'inputs'/f'{n}.csv').write_bytes(raw)
  datasets[str(n)]={'rows':n,'bytes':len(raw),'sha256':digest(raw)}
  for kind in ('IQR','FD'):expected_perf[n,kind]=expected(values,[rule(kind)])
 save(root/'datasets.json',datasets)
 input_manifest={p.name:{'sha256':file_sha(p),'bytes':p.stat().st_size} for p in (root/'inputs').iterdir()};save(root/'inputs-manifest.json',input_manifest)
 plan=[]
 for n in (10000,50000,200000):
  warm=[{'rows':n,'algorithm':k,'warmup':True,'repeat':0} for k in ('IQR','FD')]
  measured=[{'rows':n,'algorithm':k,'warmup':False,'repeat':rep} for k in ('IQR','FD') for rep in (1,2,3)]
  random.Random(20261021+n).shuffle(measured);plan.extend(warm+measured)
 save(root/'plan.json',plan)
 for index,item in enumerate(plan,1):
  values=data[item['rows']];value,_=trial(root,index,str(item['rows'])+'.csv',values,[rule(item['algorithm'])],expected_perf[item['rows'],item['algorithm']],image,session,deadline,dict(item,kind='performance'))
  records.append(value);print(json.dumps({'trial':index,'of':len(plan),'algorithm':item['algorithm'],'rows':item['rows'],'warmup':item['warmup'],'seconds':round(value['containerWallSeconds'],3)}),flush=True)
 groups=[]
 for n in (10000,50000,200000):
  for kind in ('IQR','FD'):
   selected=[v for v in records if v['kind']=='performance' and v['rows']==n and v['algorithm']==kind and not v['warmup']];assert len(selected)==3
   group={'rows':n,'algorithm':kind,'repeats':3}
   for field in ('containerWallSeconds','sampledCpuSecondsLowerBound','sampledMemoryPeakBytes','anomalyOutputBytes'):
    vals=[v[field] for v in selected];assert all(v is not None for v in vals);group[field]={'median':statistics.median(vals),'min':min(vals),'max':max(vals)}
   group['algorithmMs']={key:fn([v['ruleTimings'][0]['algorithmMs'] for v in selected]) for key,fn in [('median',statistics.median),('min',min),('max',max)]};groups.append(group)
 after=primary_fingerprints();save(root/'primary-after.json',after);assert before==after
 assert all(file_sha(ROOT/name)==h for name,h in sources.items()),'Experiment source changed'
 for n,meta in input_manifest.items():assert file_sha(root/'inputs'/n)==meta['sha256']
 save(root/'records.json',records);save(root/'summary.json',{'ok':True,'effectScenarios':len(effects),'performanceFormalRuns':18,'performanceWarmups':6,'totalSparkRuns':len(records),'groups':groups,'primaryVolumesUnchanged':True,'noCrossHostOrThroughputClaim':True})
 save(root/'COMPLETE',{'summarySha256':file_sha(root/'summary.json'),'recordsSha256':file_sha(root/'records.json'),'effectsSha256':file_sha(root/'effect-results.json')});print(json.dumps({'complete':True,'output':str(root)}),flush=True)
if __name__=='__main__':main()
