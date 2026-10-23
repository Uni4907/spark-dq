#!/usr/bin/env python3
"""Render descriptive paired header optimization observations; Matplotlib is a development dependency."""
from pathlib import Path
import argparse,json,hashlib,os

def digest(path):return {'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'size':path.stat().st_size}
def verified(root):
 complete=json.loads((root/'COMPLETE').read_text())
 for name in ('summary','measurements','compatibility'):
  if digest(root/(name+'.json'))!=complete[name]:raise ValueError('Evidence checksum mismatch: '+name)
 summary=json.loads((root/'summary.json').read_text())
 if not summary.get('ok'):raise ValueError('Incomplete experiment')
 return json.loads((root/'measurements.json').read_text())
def render(root,out):
 records=verified(root)
 os.environ.setdefault('MPLCONFIGDIR',str(out.parent/'.header-plot-cache'))
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 out.mkdir(exist_ok=False)
 fig,axes=plt.subplots(2,2,figsize=(10,6),sharex=True,layout='constrained')
 colors={'baseline':'#c87522','candidate':'#226da8'}
 for column,mode in enumerate(('local','cluster')):
  for row,metric in enumerate(('wall','read')):
   ax=axes[row,column]
   for i,profile in enumerate(('extended','governance')):
    by_variant={}
    for variant,offset in (('baseline',-.16),('candidate',.16)):
     samples=sorted([v for v in records if not v['warmup'] and (v['mode'],v['profile'],v['variant'])==(mode,profile,variant)],key=lambda v:v['repeat'])
     values=[v['containerWallSeconds'] if metric=='wall' else v['driverExecution']['inputReadAndCountMs']/1000 for v in samples]
     import statistics
     x=i+offset
     ax.scatter([x]*len(values),values,color=colors[variant],s=34,label=variant if i==0 else None,zorder=3)
     ax.hlines(statistics.median(values),x-.09,x+.09,color=colors[variant],linewidth=3,zorder=4)
     by_variant[variant]=(x,values)
    for a,b in zip(by_variant['baseline'][1],by_variant['candidate'][1]):ax.plot([i-.16,i+.16],[a,b],color='#aab2bb',linewidth=.8,zorder=1)
   ax.set_title(mode+' / '+('job wall time' if metric=='wall' else 'read + count stage'))
   peak=max(v['containerWallSeconds'] if metric=='wall' else v['driverExecution']['inputReadAndCountMs']/1000 for v in records if not v['warmup'] and v['mode']==mode)
   ax.set_ylabel('seconds');ax.set_xticks([0,1],['Detection','Governance + recheck']);ax.set_ylim(0,peak*1.15);ax.grid(axis='y',alpha=.2)
   ax.legend(fontsize=8,loc='lower left')
 fig.suptitle('Prepared CSV header check: paired baseline vs candidate\n200,000 rows / 4 files / n=2 per condition; points=all trials, bars=medians',fontsize=12)
 fig.savefig(out/'headers.png',dpi=180);fig.savefig(out/'headers.svg')
 provenance={'inputSha256':digest(root/'measurements.json')['sha256'],'matplotlib':matplotlib.__version__,'scope':'descriptive paired observations; no confidence interval or general speedup claim'}
 (out/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--input',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();render(args.input,args.output)
if __name__=='__main__':main()
