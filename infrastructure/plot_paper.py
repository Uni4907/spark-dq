from pathlib import Path
import sys,json,csv,os,tempfile,argparse
stage=Path(__file__).parent;sys.path.insert(0,str(stage/'plot-deps'))
os.environ.setdefault('MPLCONFIGDIR',str(Path(tempfile.gettempdir())/'sparkdq-matplotlib-cache'))
parser=argparse.ArgumentParser();parser.add_argument('--data',type=Path,default=stage/'figure-data');parser.add_argument('--output',type=Path,default=stage/'figures');args=parser.parse_args()
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
font=Path('C:/Windows/Fonts/msyh.ttc')
if font.exists():font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']=font_manager.FontProperties(fname=font).get_name()
plt.rcParams.update({'axes.spines.top':False,'axes.spines.right':False,'axes.unicode_minus':False,'font.size':10,'svg.fonttype':'none','pdf.fonttype':42})
data=args.data;out=args.output;out.mkdir(parents=True,exist_ok=True)
scale=json.loads((data/'t4-scale.json').read_text());groups=scale['derivedGroups'];summary=json.loads((data/'t9-summary.json').read_text());effects=summary['effect'];colors=['#126b82','#d37135'];chart_files=[]
def save(fig,name,caption):
 fig.text(.01,.015,caption,fontsize=8,color='#444444',ha='left',va='bottom');fig.tight_layout(rect=[0,.09,1,1]);
 for suffix in ['png','svg','pdf']:
  f=out/(name+'.'+suffix);fig.savefig(f,dpi=220,facecolor='white');chart_files.append(f.name)
 plt.close(fig)
fig,ax=plt.subplots(figsize=(8,4.5))
for mode,label,color in [('local','local[2]',colors[0]),('cluster','同机 Standalone 双 Worker',colors[1])]:
 items=[g for g in groups if g['mode']==mode];x=np.array([g['rows']/1e6 for g in items]);y=np.array([g['medianWallSeconds'] for g in items]);ax.errorbar(x,y,yerr=[[g['medianWallSeconds']-g['minWallSeconds'] for g in items],[g['maxWallSeconds']-g['medianWallSeconds'] for g in items]],fmt='o-',capsize=4,label=label,color=color)
 for xx,yy in zip(x,y):ax.annotate(f'{yy:.2f}s',(xx,yy),xytext=(5,7),textcoords='offset points',fontsize=9)
ax.set(title='历史正式规模实验：总墙钟时间',xlabel='输入规模（百万行）',ylabel='秒',xticks=[.2,1,5],ylim=(0,125));ax.legend();ax.grid(axis='y',alpha=.2)
save(fig,'01-T4历史规模耗时','T4旧JAR d9aeac665320…；每组1次预热+3次正式，中位数及[min,max]。\n单物理机、不同部署开销；不代表加速，也不与T7/T9新JAR功能耗时混合。')
fig,axes=plt.subplots(1,2,figsize=(10,4.5))
for ax,key,title,unit in [(axes[0],'sampledAggregateMemoryPeakMiB','内存离散采样峰值','MiB'),(axes[1],'sampledCpuSecondsLowerBound','CPU累计采样下界','CPU秒')]:
 for mode,label,color in [('local','local[2]',colors[0]),('cluster','同机双 Worker',colors[1])]:
  items=[g for g in groups if g['mode']==mode];ax.plot([g['rows']/1e6 for g in items],[g[key]['median'] for g in items],'o-',color=color,label=label)
 ax.set(title=title,xlabel='输入规模（百万行）',ylabel=unit,xticks=[.2,1,5]);ax.legend();ax.grid(axis='y',alpha=.2)
save(fig,'02-T4历史资源采样','旧JAR d9aeac…，各组3次正式值的中位数。内存不是精确堆/RSS峰值，CPU是采样下界。\nlocal仅Driver；Standalone包括Driver/Master/双Worker，配额与开销详见环境表。')
keys=['MISSING_modified_row_targets','IQR_modified_row_targets','FD_modified_row_targets','FD_rule_participant_targets','DUPLICATE_inserted_row_targets','DUPLICATE_rule_participant_targets'];labels=['缺失：注入行','IQR：注入行','FD：修改行','FD：冲突参与行','重复：新增副本','重复：全部参与行']
fig,ax=plt.subplots(figsize=(10,5));x=np.arange(len(keys));width=.25
for j,(key,label,color) in enumerate([('precision','Precision','#126b82'),('recall','Recall','#d37135'),('f1','F1','#62864b')]):
 vals=[effects[k][key] or 0 for k in keys];bars=ax.bar(x+(j-1)*width,vals,width,color=color,label=label)
 for bar,value in zip(bars,vals):ax.annotate(f'{100*value:.3f}%' if 0<value<.01 else f'{100*value:.1f}%',(bar.get_x()+bar.get_width()/2,value),ha='center',va='bottom',fontsize=7,rotation=90 if value>.1 else 0)
ax.set(title='公开业务数据上的人工注入目标识别效果',xticks=x,xticklabels=labels,ylabel='比例',ylim=(0,1.28));ax.legend(loc='upper right',ncol=3);ax.grid(axis='y',alpha=.2)
save(fig,'03-T9注入标签效果','标签是明确的受控注入，不是真实业务质量真值。FD的月→季节字段由实验构造。\nFP=不在注入标签内的预测行；不能据此断言原始记录错误。重复使用行出现次数，保留多重性。')
fig,axes=plt.subplots(2,2,figsize=(9,6.3));stages=['baseline','injected','governance','recheck'];titles=['原始增强基线','注入后','治理版本','独立复检']
for ax,kind,label in zip(axes.flat,['MISSING','DUPLICATE','IQR','FD'],['缺失 age','重复 row_id','IQR age','FD month→实验季节']):
 vals=[summary['stages'][s]['rules'][kind]['fullAnomalyRows'] for s in stages];ax.bar(titles,vals,color=['#9ca5ab','#d37135','#126b82','#62864b']);ax.set(title=label,ylabel='规则异常参与行');ax.set_ylim(0,max(1,max(vals))*1.25)
 for i,v in enumerate(vals):ax.text(i,v,str(v),ha='center',va='bottom',fontsize=9)
 ax.tick_params(axis='x',labelsize=8);ax.grid(axis='y',alpha=.15)
save(fig,'04-T9治理与独立复检','填充60个注入空白年龄为测试常量40，并按row_id去除20个副本，45231→45211行。\n没有修正999年龄或FD冲突；异常减少不能解释为全面质量改善。各规则可以重叠，不合计。')
with (out/'T4性能与资源.csv').open('w',encoding='utf-8-sig',newline='') as f:
 writer=csv.DictWriter(f,fieldnames=['rows','mode','n','medianSeconds','minSeconds','maxSeconds','cpuSecondsLowerBoundMedian','sampledMemoryPeakMiBMedian','shuffleReadBytesMedian','shuffleWriteBytesMedian','diskSpillBytesMedian','memorySpillBytesMedian']);writer.writeheader()
 for g in groups:writer.writerow(dict(rows=g['rows'],mode=g['mode'],n=3,medianSeconds=g['medianWallSeconds'],minSeconds=g['minWallSeconds'],maxSeconds=g['maxWallSeconds'],cpuSecondsLowerBoundMedian=g['sampledCpuSecondsLowerBound']['median'],sampledMemoryPeakMiBMedian=g['sampledAggregateMemoryPeakMiB']['median'],shuffleReadBytesMedian=g['shuffleReadBytes']['median'],shuffleWriteBytesMedian=g['shuffleWriteBytes']['median'],diskSpillBytesMedian=g['Disk Bytes Spilled']['median'],memorySpillBytesMedian=g['Memory Bytes Spilled']['median']))
with (out/'T9注入效果.csv').open('w',encoding='utf-8-sig',newline='') as f:
 writer=csv.DictWriter(f,fieldnames=['target','tp','fp','fn','precision','recall','f1','predictedRows','labelRows']);writer.writeheader()
 for key in keys:writer.writerow(dict(target=key,**effects[key]))
(out/'plot-environment.json').write_text(json.dumps(dict(python=sys.version,matplotlib=matplotlib.__version__,numpy=np.__version__,font=str(font),figures=chart_files,inputs={f.name:__import__('hashlib').sha256(f.read_bytes()).hexdigest() for f in data.glob('*.json')}),indent=2));print(json.dumps(dict(figures=chart_files)),flush=True)
