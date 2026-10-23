"""Derived summaries only: original measurements and outputs remain untouched."""
import json,statistics,csv
from pathlib import Path
def derive(root):
 summary=json.loads((root/'summary.json').read_text());records=json.loads((root/'measurements.json').read_text());enriched=[]
 for record in records:
  name=record['mode']+'-'+str(record['rows'])+'-'+str(record['index']);folder=root/'output'/name;result=json.loads((folder/'result/result.json').read_text());derived=dict(record)
  derived['ruleTimings']=[{k:r.get(k) for k in ('ruleId','algorithmMs','anomalyWriteMs')} for r in result['results']]
  raw=json.loads((folder/'resource-samples.json').read_text());last={};baseline=raw.get('baselineReportedDeviceIo',{})
  for sample in raw['samples']:
   for role,value in sample.get('reportedDeviceIo',{}).items():last[role]=value
  derived['sampledReportedDeviceIoDelta']={role:{key:value-baseline.get(role,{}).get(key,0) for key,value in current.items()} for role,current in last.items()}
  cache=[x for x in record['eventMetrics']['sqlExecutions'] if x.get('containsCache') and 'count at Engine.java' in x.get('description','')]
  derived['inputCacheCountCompositeMs']=cache[0].get('wallMilliseconds') if cache else None
  enriched.append(derived)
 groups=[]
 for base in summary['groups']:
  selected=[r for r in enriched if r['rows']==base['rows'] and r['mode']==base['mode'] and not r['warmup'] and not r.get('trialOnly')];assert len(selected)==3
  values=dict(base)
  cache_values=[r['inputCacheCountCompositeMs'] for r in selected if r['inputCacheCountCompositeMs'] is not None]
  values['inputCacheCountCompositeMs']=dict(median=statistics.median(cache_values),minimum=min(cache_values),maximum=max(cache_values)) if cache_values else None
  for key,reader in [('sampledCpuSecondsLowerBound',lambda r:r['resources']['sampledCpuSecondsLowerBound']),('sampledAggregateMemoryPeakMiB',lambda r:r['resources']['sampledAggregateMemoryPeakBytes']/1024**2),('verificationAndSortSeconds',lambda r:r['verificationAndSortSeconds'])]:
   measured=[reader(r) for r in selected];values[key]=dict(median=statistics.median(measured),minimum=min(measured),maximum=max(measured))
  values['rules']={}
  for rule in ('iqr','fd'):
   values['rules'][rule]={}
   for field in ('algorithmMs','anomalyWriteMs'):
    measured=[next(t[field] for t in r['ruleTimings'] if t['ruleId']==rule) for r in selected];values['rules'][rule][field]=dict(median=statistics.median(measured),minimum=min(measured),maximum=max(measured))
  for key in ('shuffleReadBytes','shuffleWriteBytes','Disk Bytes Spilled','Memory Bytes Spilled','Executor Run Time','Executor CPU Time','JVM GC Time','outputBytes','inputBytes'):
   measured=[r['eventMetrics']['taskMetrics'].get(key,0) for r in selected];target={'inputBytes':'eventInputBytes','outputBytes':'eventOutputBytes'}.get(key,key);values[target]=dict(median=statistics.median(measured),minimum=min(measured),maximum=max(measured))
  groups.append(values)
 result=dict(summary,derivedGroups=groups,notes=['Wall seconds include container/JVM startup. Algorithm and anomaly-write milliseconds come from Engine actions; write time may include upstream recomputation, not only I/O.','Cache/count composite includes input reading, cache materialization and counting; it is not pure cache cost. SQL plans/stages provide sorting, Exchange and cache context; overlapping plan categories are not summed.','Cgroup CPU and endpoint I/O are lower-bound observations; memory is a sampled concurrent aggregate, not an exact peak.','Local measurements exclude idle cluster daemons; cluster measurements include Driver/Master/Workers. Workload CPU quotas sum to 2 in each mode; cluster Master adds 0.5.'])
 (root/'derived-records.json').write_text(json.dumps(enriched,ensure_ascii=False,indent=2));(root/'derived-summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
 with (root/'derived-summary.csv').open('w',newline='') as stream:
  columns=['rows','inputBytes','mode','formalMeasurements','medianWallSeconds','minWallSeconds','maxWallSeconds','sampledCpuSecondsLowerBound','sampledAggregateMemoryPeakMiB','iqrAlgorithmMs','iqrWriteMs','fdAlgorithmMs','fdWriteMs','shuffleReadBytes','shuffleWriteBytes','diskSpillBytes'];writer=csv.DictWriter(stream,fieldnames=columns);writer.writeheader()
  for g in groups:
   writer.writerow({**{k:g[k] for k in columns[:7]},'sampledCpuSecondsLowerBound':g['sampledCpuSecondsLowerBound']['median'],'sampledAggregateMemoryPeakMiB':g['sampledAggregateMemoryPeakMiB']['median'],'iqrAlgorithmMs':g['rules']['iqr']['algorithmMs']['median'],'iqrWriteMs':g['rules']['iqr']['anomalyWriteMs']['median'],'fdAlgorithmMs':g['rules']['fd']['algorithmMs']['median'],'fdWriteMs':g['rules']['fd']['anomalyWriteMs']['median'],'shuffleReadBytes':g['shuffleReadBytes']['median'],'shuffleWriteBytes':g['shuffleWriteBytes']['median'],'diskSpillBytes':g['Disk Bytes Spilled']['median']})
 return result
