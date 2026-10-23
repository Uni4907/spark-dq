#!/usr/bin/env python3
"""Refresh bounded recognition-effect evidence with the current candidate JAR."""
from pathlib import Path
import json,os,sys,time,uuid,hashlib,subprocess
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'infrastructure'),str(ROOT/'tests')]
import algorithm_experiments as experiment
from algorithm_oracle import DOMAIN,blank
from iqr_acceptance import csv_bytes
def main():
 assert not experiment.command(['docker','ps','-q']).strip(),'Wait until scale experiment stops'
 root=ROOT/'.runtime'/('t5-effects-'+time.strftime('%Y%m%d-%H%M%S'));root.mkdir(mode=0o755);(root/'inputs').mkdir();(root/'trials').mkdir()
 candidate=json.loads((ROOT/'.runtime/t1-build-20261021-165747/candidate.json').read_text());image=candidate['backendImageId'];actual=experiment.command(['docker','run','--rm','--network','none','--entrypoint','sha256sum',image,'/app/engine/spark-engine-0.1.0.jar']).decode().split()[0];assert actual==candidate['engineJarSha256']
 rows,clean,rules,labels=experiment.effect_data();(root/'inputs/effect.csv').write_bytes(csv_bytes(list(rows[0]),rows));(root/'inputs/effect-clean.csv').write_bytes(csv_bytes(list(clean[0]),clean));experiment.save(root/'labels.json',labels)
 oracle=experiment.expected(rows,rules);experiment.save(root/'oracle.json',{k:{'metrics':v[0],'anomalyIds':[rows[i]['row_id'] for i in v[1]]} for k,v in oracle.items()});experiment.save(root/'environment.json',dict(imageId=image,engineJarSha256=actual,rows=1000,rules=len(rules),seeds=[20261021,20261022,20261023],cpuQuota=2,memoryBytes=2*1024**3,heapMiB=1024,execution='local[2]',scope='Fixed small synthetic recognition-effect cases, independent from scale experiment; no general accuracy claim'))
 record,predictions=experiment.trial(root,0,'effect.csv',rows,rules,oracle,image,uuid.uuid4().hex,time.monotonic()+300,dict(kind='effect-refresh',rows=1000,warmup=False));effects=[]
 for label,rule in zip(labels,rules):
  eligible={v['row_id'] for v in rows if v[rule['columns'][0]] is not None and DOMAIN.fullmatch(v[rule['columns'][0]])} if rule['type']=='IQR' else {v['row_id'] for v in rows if not any(blank(v[c]) for c in rule['columns']+rule['params']['dependentColumns'])}
  effect={k:v for k,v in label.items() if k not in ('positiveIds','modifiedIds')};effect.update(experiment.confusion(predictions[rule['id']],label['positiveIds'],eligible));effect['skipped']=len(rows)-len(eligible)
  if rule['type']=='FD':effect['modifiedRowLabelsAlternative']=experiment.confusion(predictions[rule['id']],label['modifiedIds'],eligible)
  effects.append(effect)
 experiment.save(root/'effects.json',effects);experiment.save(root/'measurement.json',record)
 assert any(e['scenario']=='skewed' and e['FP']>0 for e in effects)
 assert all(e['recall']==0 for e in effects if e['scenario'] in ('contamination30','subtle'))
 summary=dict(ok=True,rows=1000,rules=len(rules),effectScenarios=len(effects),seeds=3,engineJarSha256=actual,skewedFalsePositives=[e['FP'] for e in effects if e['scenario']=='skewed'],zeroRecallScenarios=[dict(seed=e['seed'],scenario=e['scenario'],FN=e['FN'],recall=e['recall']) for e in effects if e['scenario'] in ('contamination30','subtle')],fdParticipationAndModifiedLabelsReportedSeparately=True,fullAnomalyMultisetsVerified=True)
 experiment.save(root/'summary.json',summary);print(json.dumps(dict(root=str(root),**summary)),flush=True)
if __name__=='__main__':main()
