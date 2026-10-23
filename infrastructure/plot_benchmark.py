#!/usr/bin/env python3
"""Standalone scientific figures from a COMPLETE benchmark; matplotlib is a dev-only dependency."""
import argparse,hashlib,json
from pathlib import Path

def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('experiment',type=Path);parser.add_argument('--output',type=Path,required=True)
 args=parser.parse_args()
 raw=(args.experiment/'summary.json').read_bytes()
 marker=json.loads((args.experiment/'COMPLETE').read_text())
 if hashlib.sha256(raw).hexdigest()!=marker['summarySha256']:raise ValueError('Summary checksum mismatch')
 summary=json.loads(raw)
 if not summary['complete']:raise ValueError('Incomplete measurements cannot produce final figures')
 if args.output.exists():raise ValueError('Refusing to overwrite an existing figure directory')
 args.output.mkdir(parents=True)
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,
  'svg.fonttype':'path','pdf.fonttype':42,'savefig.dpi':180})
 colors={1:'#2457a6',2:'#14856f',4:'#cf7625'}
 profiles=[('basic','Basic detection (3 rules)'),('extended','Extended detection (7 rules)'),('governance','Governance + recheck (7 rules)')]
 groups=summary['groups']
 for metric,name,ylabel in [('medianWallSeconds','latency','Container wall time (s)'),('medianPeakRssMiB','memory','JVM process peak RSS at result (MiB)')]:
  fig,axes=plt.subplots(1,3,figsize=(12.5,4),sharex=True)
  for ax,(profile,title) in zip(axes,profiles):
   data=[row for row in groups if row['profile']==profile]
   for partitions in sorted({row['partitions'] for row in data}):
    items=sorted((row for row in data if row['partitions']==partitions),key=lambda row:row['rows'])
    x=[row['rows'] for row in items];y=[row[metric] for row in items]
    if metric=='medianWallSeconds':
     error=[[row['medianWallSeconds']-row['minWallSeconds'] for row in items],[row['maxWallSeconds']-row['medianWallSeconds'] for row in items]]
     ax.errorbar(x,y,yerr=error,marker='o',capsize=3,color=colors.get(partitions),label=f'Input partitions: {partitions}')
    else:ax.plot(x,y,marker='o',color=colors.get(partitions),label=f'Input partitions: {partitions}')
   ax.set_xscale('log');ax.set_xticks(sorted({row['rows'] for row in data}))
   ax.set_xticklabels([f"{n/1000:g}k" for n in sorted({row['rows'] for row in data})])
   ax.set_title(title);ax.set_xlabel('Input rows');ax.set_ylabel(ylabel)
   ax.set_xlim(min(row['rows'] for row in data)/1.2,max(row['rows'] for row in data)*1.2)
   ax.grid(True,alpha=.2);ax.set_ylim(bottom=0)
  axes[0].legend(loc='best',fontsize=8)
  fig.suptitle('SparkDQ local[2] baseline: 2 CPU quota / 2 GiB container / 1 GiB heap',fontsize=12)
  note='Two measured repetitions per condition; error bars show min-max, not confidence intervals.' if metric=='medianWallSeconds' else 'RSS is Linux VmHWM sampled before Spark shutdown; it includes native memory and is not JVM heap.'
  fig.text(.5,.015,note,ha='center',fontsize=9)
  fig.tight_layout(rect=(0,.05,1,.93))
  for suffix in ('png','svg','pdf'):fig.savefig(args.output/f'{name}.{suffix}',bbox_inches='tight')
  plt.close(fig)
 print(json.dumps({'figures':str(args.output),'measuredTrials':summary['measuredTrials'],'conditions':len(groups)}))
if __name__=='__main__':main()
