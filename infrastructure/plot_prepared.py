#!/usr/bin/env python3
"""Render only completed prepared-format comparisons; plotting is development-only."""
import argparse,hashlib,json
from pathlib import Path
def render(source,output):
    raw=(source/'summary.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=json.loads((source/'COMPLETE').read_text())['summarySha256']:
        raise ValueError('Summary checksum mismatch')
    summary=json.loads(raw)
    if not summary['complete'] or not summary['sourceUnchanged']:raise ValueError('Incomplete experiment')
    output.mkdir()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    groups=summary['groups'];conditions=sorted({(g['rows'],g['profile']) for g in groups})
    kinds=['CSV_SINGLE','CSV','PARQUET'];labels=['Single CSV','4-part CSV','4-part Parquet'];colors=['#386cb0','#e2912e','#3c9571']
    figure,axes=plt.subplots(1,2,figsize=(12.8,5.3),layout='constrained')
    positions=np.arange(len(conditions));width=.23
    for i,(kind,label,color) in enumerate(zip(kinds,labels,colors)):
        selected=[next(g for g in groups if (g['rows'],g['profile'],g['format'])==(n,profile,kind)) for n,profile in conditions]
        walls=[g['medianWallSeconds'] for g in selected]
        errors=[[g['medianWallSeconds']-g['minWallSeconds'] for g in selected],[g['maxWallSeconds']-g['medianWallSeconds'] for g in selected]]
        x=positions+(i-1)*width
        axes[0].bar(x,walls,width,color=color,label=label,yerr=errors,capsize=3)
        axes[1].bar(x,[g['preparePlusOneRunSeconds'] for g in selected],width,color=color,label=label)
    for axis in axes:
        axis.set_xticks(positions,[f'{n:,}\n{profile}' for n,profile in conditions])
        axis.set_ylabel('Seconds');axis.set_ylim(bottom=0);axis.grid(axis='y',alpha=.2);axis.set_axisbelow(True)
    axes[0].set_title('Worker only: median and min/max (n=2)')
    axes[1].set_title('Preparation + one median worker run')
    axes[0].legend(frameon=False,fontsize=9)
    figure.suptitle('SparkDQ format comparison | local[2], 2 CPU, 2 GiB container\nFresh JVM each trial; host cache uncontrolled; no cluster claim',fontsize=12)
    plt.rcParams['svg.fonttype']='path'
    for extension in ('png','svg','pdf'):figure.savefig(output/('formats.'+extension),dpi=180)
    plt.close(figure)
    (output/'provenance.json').write_text(json.dumps({'summarySha256':hashlib.sha256(raw).hexdigest(),'matplotlib':matplotlib.__version__,'numpy':np.__version__,'sampleSize':2},indent=2)+'\n')
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('source',type=Path);p.add_argument('output',type=Path);args=p.parse_args()
    render(args.source,args.output)
if __name__=='__main__':main()
