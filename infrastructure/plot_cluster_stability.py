#!/usr/bin/env python3
"""Plot completed bounded stability observations; Matplotlib is optional development tooling."""
from pathlib import Path
import argparse,hashlib,json,os

def digest(path):return {'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'size':path.stat().st_size}
def verified(root):
 complete=json.loads((root/'COMPLETE').read_text())
 for key,name in (('summary','summary.json'),('records','records.json'),('resources','resource-samples.json')):
  if digest(root/name)!=complete[key]:raise ValueError('Stability evidence checksum mismatch: '+key)
 summary=json.loads((root/'summary.json').read_text());resources=json.loads((root/'resource-samples.json').read_text())
 if not summary.get('ok') or resources.get('collectorError') or len(resources.get('samples',[]))<2:raise ValueError('Incomplete resource observation')
 return summary,resources['samples']
def render(root,output):
 summary,samples=verified(root);output.mkdir(exist_ok=False);os.environ.setdefault('MPLCONFIGDIR',str(output.parent/'.stability-plot-cache'))
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 fig,axes=plt.subplots(2,1,figsize=(11,6),sharex=True,layout='constrained');times=[value['seconds']/60 for value in samples]
 roles=sorted(samples[0]['roles'])
 for role in roles:
  axes[0].plot(times,[value['roles'][role]['memoryBytes']/1024**2 for value in samples],label=role,linewidth=1)
 axes[0].set_ylabel('cgroup memory.current (MiB)');axes[0].set_ylim(bottom=0);axes[0].legend(fontsize=8,loc='upper left',bbox_to_anchor=(1.01,1));axes[0].margins(y=.15)
 baseline=sum(v['cpuUsec'] for v in samples[0]['roles'].values());axes[1].plot(times,[(sum(v['cpuUsec'] for v in value['roles'].values())-baseline)/1e6 for value in samples],color='#246a9c',label='six-service cumulative CPU')
 axes[1].set_ylabel('sampled CPU seconds');axes[1].set_xlabel('elapsed minutes');axes[1].set_ylim(bottom=0);axes[1].legend(fontsize=8,loc='upper left')
 for path in sorted(root.glob('idle-*.json')):
  idle=json.loads(path.read_text());minute=idle['seconds']/60
  for ax in axes:ax.axvline(minute,color='#abb6bb',alpha=.5,linewidth=.65)
 for ax in axes:ax.grid(axis='y',alpha=.2);ax.margins(x=.01)
 fig.suptitle('Bounded repeated HTTP lifecycle observation\n'+str(summary['successfulHttpTasks'])+' successful tasks + 2 cancellations; 10k rows; vertical lines = idle checks',fontsize=12)
 fig.savefig(output/'stability.png',dpi=180);fig.savefig(output/'stability.svg')
 (output/'provenance.json').write_text(json.dumps({'resourceSha256':digest(root/'resource-samples.json')['sha256'],'matplotlib':matplotlib.__version__,'scope':'memory.current includes file cache; descriptive single-host observation, not proof of no memory leaks'},indent=2)+'\n')
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--input',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();render(args.input,args.output)
if __name__=='__main__':main()
