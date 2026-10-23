#!/usr/bin/env python3
"""Plot only a COMPLETE paired comparison; plotting dependencies remain development-only."""
import argparse,hashlib,json
from pathlib import Path

def verified(root):
 complete=json.loads((root/'COMPLETE').read_text())
 for name,key in [('summary.json','summary'),('measurements.json','measurements')]:
  raw=(root/name).read_bytes();assert {'sha256':hashlib.sha256(raw).hexdigest(),'size':len(raw)}==complete[key],'Evidence checksum mismatch'
 value=json.loads((root/'summary.json').read_text());assert value['ok'] and value['measuredTrials']>0;return value

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('input',type=Path);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();data=verified(args.input);assert not args.output.exists()
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 import numpy as np
 groups=data['groups'];cases=sorted({(row['rows'],row['profile']) for row in groups});assert all(row['n']>=1 for row in groups)
 fig,axes=plt.subplots(1,3,figsize=(15,4.8),layout='constrained');colors=['#167d8d','#d58b40'];x=np.arange(len(cases));width=.36
 for axis,key,title in zip(axes,['medianWallSeconds','medianSampledCpuSecondsLowerBound','medianSampledMemoryPeakMiB'],['Elapsed time (s)','Sampled CPU lower bound (s)','Sampled aggregate memory peak (MiB)']):
  for j,mode in enumerate(('local','cluster')):
   rows=[next(row for row in groups if (row['rows'],row['profile'],row['mode'])==(n,p,mode)) for n,p in cases];values=[row[key] for row in rows];offset=x+(j-.5)*width
   errors=[[row[key]-row['minWallSeconds'] for row in rows],[row['maxWallSeconds']-row[key] for row in rows]] if key=='medianWallSeconds' else None
   bars=axis.bar(offset,values,width,color=colors[j],label='Local' if j==0 else 'Standalone (incl. Master)',yerr=errors,capsize=3)
   axis.bar_label(bars,fmt='%.1f',padding=3,fontsize=8)
  axis.set_title(title,fontsize=11);axis.set_xticks(x,[str(n//1000)+'k\n'+('Detection' if p=='extended' else 'Governance') for n,p in cases],fontsize=8);axis.set_ylim(0,axis.get_ylim()[1]*1.16);axis.grid(axis='y',alpha=.2);axis.set_axisbelow(True)
 handles,labels=axes[0].get_legend_handles_labels();fig.legend(handles,labels,loc='outside lower center',ncols=2,frameon=False,fontsize=9)
 sizes=','.join(str(n) for n in sorted({row['n'] for row in groups}));fig.suptitle(f'SparkDQ: paired same-host comparison | n={sizes} per condition; median; time whiskers=min-max\nWorkload quotas: 2 CPU / 5 GiB; Standalone adds Master 0.5 CPU / 512 MiB; no speedup claim',fontsize=11)
 args.output.mkdir(parents=True)
 for suffix in ('png','svg'):fig.savefig(args.output/('comparison.'+suffix),dpi=180)
 plt.close(fig)
 (args.output/'provenance.json').write_text(json.dumps({'summarySha256':hashlib.sha256((args.input/'summary.json').read_bytes()).hexdigest(),'matplotlib':matplotlib.__version__,'numpy':np.__version__,'measuredTrials':data['measuredTrials'],'scope':data['scope']},indent=2)+'\n')
if __name__=='__main__':main()
