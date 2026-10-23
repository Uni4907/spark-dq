"""Disk-backed synthetic scale oracle. Memory does not grow with row count."""
import csv,hashlib,json,sqlite3,subprocess,time,resource
from decimal import Decimal,localcontext
from pathlib import Path
COLUMNS=['id','n','x','y','padding']
RULES=[{'id':'iqr','type':'IQR','columns':['n'],'params':{'k':'1.5'}},{'id':'fd','type':'FD','columns':['x'],'params':{'dependentColumns':['y']}}]
def row(i):
 j=i//2;base=9007199254740993
 value=str(base+(100000 if j%1000==0 else -100000 if j%500==1 else (j%100)**2))+'.0000000001'
 if j%997==0 and j%1000!=0:value='bad'
 if j%991==0 and j%1000!=0:value=None
 x='u'+str(j) if j%4==0 else 'hot' if j%4==1 else 'g'+str(j//10)
 if j%101==99:x=None
 return dict(id=str(j),n=value,x=x,y='modified' if j%10==3 else 'a',padding='repeat')
def encode(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n'
def sorted_digest(source,destination):
 subprocess.run(['sort','--parallel=1','-S','64M','-T',str(destination.parent),'-o',str(destination),str(source)],env={'LC_ALL':'C','PATH':'/usr/bin:/bin'},check=True,timeout=180)
 digest=hashlib.sha256();count=0
 with destination.open('rb') as stream:
  for line in stream:digest.update(line);count+=1
 return {'sha256':digest.hexdigest(),'rows':count,'bytes':destination.stat().st_size}
def prepare(root,n):
 root.mkdir();started=time.monotonic();hist={};blank=bad=0
 db=sqlite3.connect(root/'groups.sqlite');db.execute('pragma cache_size=-65536');db.execute('pragma temp_store=FILE');db.execute('pragma journal_mode=OFF');db.execute('create table groups(x text primary key,y text,conflict integer,c integer) without rowid')
 with (root/'input.csv').open('w',newline='',encoding='utf-8') as stream:
  writer=csv.writer(stream,quoting=csv.QUOTE_ALL,lineterminator='\n');writer.writerow(COLUMNS)
  for i in range(n):
   value=row(i);writer.writerow(['\\N' if value[c] is None else value[c] for c in COLUMNS]);v=value['n']
   if v is None:blank+=1
   elif v=='bad':bad+=1
   else:hist[Decimal(v)]=hist.get(Decimal(v),0)+1
   if value['x'] is not None:db.execute('insert into groups values(?,?,0,1) on conflict(x) do update set conflict=conflict or y<>excluded.y,c=c+1',(value['x'],value['y']))
 db.commit();generation=time.monotonic()-started;valid=sum(hist.values());pairs=sorted(hist.items())
 def point(index):
  for value,count in pairs:
   if index<count:return value
   index-=count
  raise ValueError('rank')
 with localcontext() as ctx:
  ctx.prec=80
  def quantile(numerator):
   whole,remainder=divmod((valid-1)*numerator,4);return point(whole)+(point(whole+1)-point(whole))*Decimal(remainder)/4 if remainder else point(whole)
  q1=quantile(1);q3=quantile(3);span=q3-q1;lower=q1-Decimal('1.5')*span;upper=q3+Decimal('1.5')*span
  def plain(v):return format(v.normalize(),'f')
  iq=dict(algorithmVersion='iqr-exact-decimal-linear-v1',status='ZERO_IQR' if span==0 else 'EVALUATED',numericDomain='DECIMAL_28_10_NO_EXPONENT_NO_TRIM',minimumValidCount=4,k='1.5',checkedCount=valid,evaluatedCount=valid,skippedCount=n-valid,skippedBlankCount=blank,nonNumericCount=bad,outOfDomainCount=0,q1=plain(q1),q3=plain(q3),iqr=plain(span),lowerBound=plain(lower),upperBound=plain(upper))
  groups,conflicts,participation,checked=db.execute('select count(*),coalesce(sum(conflict),0),coalesce(sum(case when conflict=1 then c else 0 end),0),sum(c) from groups').fetchone()
  fd=dict(algorithmVersion='fd-exact-text-skip-blank-v1',status='EVALUATED',checkedCount=checked,skippedCount=n-checked,skippedBlankCount=n-checked,skippedNullCount=n-checked,skippedEmptyCount=0,skippedWhitespaceCount=0,checkedGroups=groups,conflictGroups=conflicts,conflictRows=participation,invalidCount=participation)
  iq_count=0
  with (root/'iqr.expected.unsorted').open('w') as iout,(root/'fd.expected.unsorted').open('w') as fout:
   for i in range(n):
    value=row(i);v=value['n']
    if v is not None and v!='bad' and (Decimal(v)<lower or Decimal(v)>upper):iout.write(encode(value));iq_count+=1
    if value['x'] is not None and db.execute('select conflict from groups where x=?',(value['x'],)).fetchone()[0]:fout.write(encode(value))
  iq['invalidCount']=iq_count
 db.close();digests={name:sorted_digest(root/(name+'.expected.unsorted'),root/(name+'.expected.sorted')) for name in ('iqr','fd')}
 assert digests['fd']['rows']==fd['invalidCount'] and digests['iqr']['rows']==iq['invalidCount']
 result=dict(rows=n,inputBytes=(root/'input.csv').stat().st_size,metrics={'iqr':iq,'fd':fd},anomalies=digests,generationSeconds=generation,oracleAndSortSeconds=time.monotonic()-started-generation,oracleProcessLifetimeMaxRssKiB=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,memoryPolicy='Streaming CSV/JSON; <=102-value Decimal histogram; SQLite 64MiB cache; external sort 64MiB. No full input list or Counter.')
 (root/'expected.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));return result
