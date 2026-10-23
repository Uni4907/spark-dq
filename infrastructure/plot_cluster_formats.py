#!/usr/bin/env python3
"""Plot verified format comparisons and explicit preparation cost, no production cache claim."""
import argparse,hashlib,json
from pathlib import Path
from plot_cluster_compare import verified

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('input',type=Path);p.add_argument('--output',type=Path,required=True);args=p.parse_args();summary=verified(args.input);assert not args.output.exists()
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 import numpy as np
 groups=summary['groups'];cases=sorted({(v['rows'],v['mode'],v['profile']) for v in groups},key=lambda key:(key[0],{'local':0,'cluster':1}[key[1]],{'extended':0,'governance':1}[key[2]]));x=np.arange(len(cases));width=.24
 fig,axes=plt.subplots(1,2,figsize=(13.5,5.4),layout='constrained');kinds=['CSV_SINGLE','CSV','PARQUET'];colors=['#167d8d','#d58b40','#6b6eaf'];labels=['Single CSV','4-part CSV','Parquet (4 files)']
 for j,(kind,color,label) in enumerate(zip(kinds,colors,labels)):
  rows=[next(v for v in groups if (v['rows'],v['mode'],v['profile'],v['format'])==(*case,kind)) for case in cases];offset=x+(j-1)*width
  for axis,key in zip(axes,['medianWallSeconds','preparePlusOneRunSeconds']):
   bars=axis.bar(offset,[v[key] for v in rows],width,color=color,label=label,yerr=[[v['medianWallSeconds']-v['minWallSeconds'] for v in rows],[v['maxWallSeconds']-v['medianWallSeconds'] for v in rows]] if key=='medianWallSeconds' else None,capsize=3);axis.bar_label(bars,fmt='%.1f',padding=3,fontsize=8)
 for axis,title in zip(axes,['Worker only: median and min/max','Preparation + one median worker run']):
  axis.set_title(title,fontsize=11);axis.set_ylabel('Seconds');axis.set_xticks(x,[str(n//1000)+'k '+('Local' if mode=='local' else 'Cluster')+'\n'+('Detection' if profile=='extended' else 'Governance') for n,mode,profile in cases],fontsize=9);axis.set_ylim(0,axis.get_ylim()[1]*1.16);axis.grid(axis='y',alpha=.2);axis.set_axisbelow(True)
 handles,legends=axes[0].get_legend_handles_labels();fig.legend(handles,legends,loc='outside lower center',ncols=3,frameon=False,fontsize=9)
 sizes=','.join(str(n) for n in sorted({v['n'] for v in groups}));fig.suptitle('SparkDQ: same-data format comparison | worker n='+sizes+' per condition\n2 CPU / 5 GiB aggregate workload quotas; Cluster adds Master\nPreparation measured once per format; reuse is a cost model, not a deployed cache',fontsize=11)
 args.output.mkdir(parents=True)
 for extension in ('png','svg'):fig.savefig(args.output/('formats.'+extension),dpi=180)
 plt.close(fig);(args.output/'provenance.json').write_text(json.dumps({'summarySha256':hashlib.sha256((args.input/'summary.json').read_bytes()).hexdigest(),'matplotlib':matplotlib.__version__,'numpy':np.__version__,'scope':summary['scope']},indent=2)+'\n')
if __name__=='__main__':main()
