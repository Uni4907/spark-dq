"""Independent stdlib oracle; never imports Spark metrics or fitted fences."""
import re
from decimal import Decimal,localcontext
DOMAIN=re.compile(r'[+-]?(?:[0-9]{1,28}(?:\.[0-9]{1,10})?|\.[0-9]{1,10})\Z')
SYNTAX=re.compile(r'[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)\Z')
def blank(value):return value is None or value.strip(' ')==''
def iqr(values,k='1.5'):
 valid=sorted(Decimal(v) for v in values if v is not None and DOMAIN.fullmatch(v))
 m={'algorithmVersion':'iqr-exact-decimal-linear-v1','status':'INSUFFICIENT_DATA','numericDomain':'DECIMAL_28_10_NO_EXPONENT_NO_TRIM','minimumValidCount':4,'k':str(Decimal(k).normalize()),'checkedCount':len(valid),'evaluatedCount':0,'skippedCount':len(values)-len(valid),'skippedBlankCount':sum(blank(v) for v in values),'nonNumericCount':sum(not blank(v) and not SYNTAX.fullmatch(v) for v in values),'q1':None,'q3':None,'iqr':None,'lowerBound':None,'upperBound':None}
 m['outOfDomainCount']=len(values)-len(valid)-m['skippedBlankCount']-m['nonNumericCount'];indexes=[]
 if len(valid)>=4:
  with localcontext() as ctx:
   ctx.prec=80
   def q(p):
    index=(len(valid)-1)*p;whole=int(index);fraction=index-whole
    return valid[whole] if fraction==0 else valid[whole]+fraction*(valid[whole+1]-valid[whole])
   q1=q(Decimal('.25'));q3=q(Decimal('.75'));span=q3-q1;lo=q1-Decimal(k)*span;hi=q3+Decimal(k)*span
   def text(n):return format(n.normalize(),'f')
   m.update(status='ZERO_IQR' if span==0 else 'EVALUATED',evaluatedCount=len(valid),q1=text(q1),q3=text(q3),iqr=text(span),lowerBound=text(lo),upperBound=text(hi))
   indexes=[i for i,v in enumerate(values) if v is not None and DOMAIN.fullmatch(v) and (Decimal(v)<lo or Decimal(v)>hi)]
 m['invalidCount']=len(indexes);return m,indexes
def fd(rows,x,y):
 groups={};skipped=0;null=empty=space=0
 for i,row in enumerate(rows):
  values=[row.get(c) for c in x+y]
  null+=any(v is None for v in values);empty+=any(v=='' for v in values);space+=any(v is not None and v!='' and v.strip(' ')=='' for v in values)
  if any(blank(v) for v in values):skipped+=1;continue
  groups.setdefault(tuple(row[c] for c in x),[]).append((i,tuple(row[c] for c in y)))
 conflicts=[members for members in groups.values() if len({v for _,v in members})>1];indexes=sorted(i for g in conflicts for i,_ in g)
 return {'algorithmVersion':'fd-exact-text-skip-blank-v1','status':'NO_VALID_ROWS' if skipped==len(rows) else 'EVALUATED','checkedCount':len(rows)-skipped,'skippedCount':skipped,'skippedBlankCount':skipped,'skippedNullCount':null,'skippedEmptyCount':empty,'skippedWhitespaceCount':space,'checkedGroups':len(groups),'conflictGroups':len(conflicts),'conflictRows':len(indexes),'invalidCount':len(indexes)},indexes
