#!/usr/bin/env python3
"""UCI public data with original bytes, explicit derivations, controlled injection log."""
from pathlib import Path
import urllib.request,zipfile,io,csv,json,hashlib,time,collections
P=Path('/home/sparkdq/projects/spark-dq');root=P/'.runtime'/('t9-public-'+time.strftime('%Y%m%d-%H%M%S'));root.mkdir(mode=0o700);source=root/'source';source.mkdir()
url='https://archive.ics.uci.edu/static/public/222/bank%2Bmarketing.zip'
with urllib.request.urlopen(url,timeout=45) as response:
 raw=response.read(2*1024**2+1);assert len(raw)<=2*1024**2
(source/'bank-marketing.zip').write_bytes(raw)
with zipfile.ZipFile(io.BytesIO(raw)) as outer:
 names=outer.namelist();name=next(n for n in names if n.endswith('bank.zip'));bank=outer.read(name);assert len(bank)<1024**2
(source/'bank.zip').write_bytes(bank)
with zipfile.ZipFile(io.BytesIO(bank)) as z:
 for name in ['bank-full.csv','bank-names.txt']:
  actual=next(n for n in z.namelist() if n.endswith(name));content=z.read(actual);assert len(content)<8*1024**2;(source/name).write_bytes(content)
with (source/'bank-full.csv').open(encoding='utf-8',newline='') as stream:original=list(csv.DictReader(stream,delimiter=';'))
assert len(original)==45211 and len(original[0])==17
season={m:s for ms,s in [('dec jan feb','winter'),('mar apr may','spring'),('jun jul aug','summer'),('sep oct nov','autumn')] for m in ms.split()}
rows=[dict(row_id='r'+str(i).zfill(5),**row,dq_month_season=season[row['month']]) for i,row in enumerate(original)]
def write(name,values):
 with (root/name).open('x',encoding='utf-8',newline='') as f:
  writer=csv.DictWriter(f,fieldnames=list(rows[0]),quoting=csv.QUOTE_ALL,lineterminator='\r\n');writer.writeheader();writer.writerows({k:'\\N' if v is None else v for k,v in row.items()} for row in values)
write('baseline.csv',rows);changes=[];injected=[dict(r) for r in rows]
for i in range(100,160):
 value=[None,'','   '][(i-100)%3];changes.append(dict(kind='MISSING',row_id=rows[i]['row_id'],field='age',before=rows[i]['age'],after=value,lineIndex=i));injected[i]['age']=value
for i in range(200,240):
 changes.append(dict(kind='IQR',row_id=rows[i]['row_id'],field='age',before=rows[i]['age'],after='999',lineIndex=i));injected[i]['age']='999'
for month in season:
 indices=[i for i,r in enumerate(rows) if i>=300 and r['month']==month][:2];assert len(indices)==2
 for i in indices:
  changes.append(dict(kind='FD',row_id=rows[i]['row_id'],field='dq_month_season',before=rows[i]['dq_month_season'],after='CONTROLLED_CONFLICT',lineIndex=i));injected[i]['dq_month_season']='CONTROLLED_CONFLICT'
for i in range(20):
 changes.append(dict(kind='DUPLICATE_INSERTION',row_id=rows[i]['row_id'],originalLineIndex=i,insertedLineIndex=len(injected)));injected.append(dict(injected[i]))
write('injected.csv',injected)
governed=[];seen=set()
for row in injected:
 if row['row_id'] in seen:continue
 seen.add(row['row_id']);row=dict(row)
 if row['age'] is None or row['age'].strip(' ')=='' :row['age']='40'
 governed.append(row)
write('governed-reference.csv',governed)
card=dict(dataset='UCI Bank Marketing (legacy bank-full.csv)',authors=['S. Moro','P. Rita','P. Cortez'],doi='10.24432/C5K306',officialPage='https://archive.ics.uci.edu/dataset/222/bank+marketing',downloadUrl=url,acquiredUtc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),license='CC BY 4.0',licenseUrl='https://creativecommons.org/licenses/by/4.0/',sourceRows=45211,originalColumns=list(original[0]),sourceFiles={f.name:dict(bytes=f.stat().st_size,sha256=hashlib.sha256(f.read_bytes()).hexdigest()) for f in source.iterdir()},transformations=['Semicolon UTF-8 CSV -> quoted comma UTF-8 CRLF CSV; original field text and row order preserved','Added row_id from zero-based source line index; NOT an original customer identifier','Added dq_month_season as explicit month->season test mapping; NOT a discovered business FD','Original unknown category text retained; subscription y is NOT a data-quality truth label'],qualityTruth='No trusted real-world quality labels. Precision/Recall/F1 concern controlled injections; non-injected flags are not established business errors.',injectionCounts=dict(collections.Counter(v['kind'] for v in changes)),seasonMapping=season,governance='Fill the 60 injected blank ages with chosen test constant40 and deduplicate row_id; does not correct999 ages or FD conflicts',columns=list(rows[0]))
(root/'data-card.json').write_text(json.dumps(card,ensure_ascii=False,indent=2));(root/'injections.json').write_text(json.dumps(changes,ensure_ascii=False,indent=2));print(root,flush=True)
