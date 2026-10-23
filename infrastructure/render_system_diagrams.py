from pathlib import Path
import sys,os,tempfile,argparse
stage=Path(__file__).parent;sys.path.insert(0,str(stage.parent/'t9/plot-deps'));os.environ.setdefault('MPLCONFIGDIR',str(Path(tempfile.gettempdir())/'sparkdq-matplotlib-cache'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch,FancyArrowPatch
from matplotlib import font_manager
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,default=stage/'figures');parser.add_argument('--font',type=Path,default=Path('C:/Windows/Fonts/msyh.ttc'));args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
if args.font.exists():font_manager.fontManager.addfont(str(args.font));plt.rcParams['font.family']=font_manager.FontProperties(fname=args.font).get_name()
plt.rcParams.update({'svg.fonttype':'none','pdf.fonttype':42})
def draw(name,title,items,edges,height=8):
 fig,ax=plt.subplots(figsize=(11,height));ax.set(xlim=(0,12),ylim=(0,height));ax.axis('off');patches={};positions={}
 for key,(x,y,label) in items.items():
  positions[key]=(x,y);patch=FancyBboxPatch((x-1.3,y-.46),2.6,.92,boxstyle='round,pad=.06,rounding_size=.09',facecolor='#edf7f8',edgecolor='#16677b',linewidth=1.3,zorder=2);ax.add_patch(patch);patches[key]=patch;ax.text(x,y,label,ha='center',va='center',fontsize=10.5,color='#173f50',zorder=3)
 for a,b in edges:ax.add_patch(FancyArrowPatch(positions[a],positions[b],patchA=patches[a],patchB=patches[b],arrowstyle='-|>',mutation_scale=14,linewidth=1.1,color='#698493',zorder=1))
 fig.suptitle(title,fontsize=18,y=.96);fig.text(.02,.025,'SparkDQ · 本机毕业设计系统；箭头为主要逻辑关系，精确接口、ER与恢复状态另见Mermaid图源。',fontsize=8,color='#536e79');fig.tight_layout(rect=[0,.05,1,.94])
 for ext in ['png','svg','pdf']:fig.savefig(args.output/(name+'.'+ext),dpi=220,facecolor='white')
 plt.close(fig)
draw('01-架构概览','SparkDQ 系统总体架构',{'browser':(6,7,'浏览器\nVue3 / ECharts'),'api':(6,5.4,'Spring Boot API\n规则 / 状态 / 版本'),'sources':(2,5.4,'CSV / Parquet / 受控文件\nMySQL / Hive固定快照'),'meta':(10,5.4,'MySQL 元数据库\n快照 / 模板 / 审计'),'validation':(2,3.5,'白名单 / 标准化\n指纹 / 输入预算'),'queue':(6,3.5,'单任务执行器\n最多8项排队'),'files':(10,3.5,'不可变文件卷\n原文 / 副本 / 完整异常'),'modes':(2,1.5,'local[2]\n同机双Worker功能兼容'),'spark':(6,1.5,'Spark Engine\nDataFrame / 精确规则'),'report':(10,1.5,'HTML / JSON报告\n历史 / 治理 / 独立复检')},[('browser','api'),('sources','validation'),('validation','api'),('api','meta'),('api','queue'),('queue','spark'),('spark','modes'),('spark','files'),('api','files'),('files','report')])
draw('02-功能结构','SparkDQ 主要功能结构',{'root':(6,6.5,'SparkDQ\n检测与治理'),'data':(2,4.6,'数据接入\nCSV / 分片 / Parquet'),'rule':(6,4.6,'规则与任务\n七类规则 / 模板版本'),'output':(10,4.6,'治理与结果\n独立版本 / 复检 / 报告'),'sources':(2,2.7,'白名单源与快照\nMySQL / 受控文件 / Hive'),'life':(6,2.7,'执行生命周期\n排队 / 取消 / 重启恢复'),'gov':(10,2.7,'可解释治理\n填充 / 映射 / 优先级去重'),'ops':(6,.9,'完整性与运维\n指纹 / 维护备份 / 新卷恢复')},[('root','data'),('root','rule'),('root','output'),('data','sources'),('rule','life'),('output','gov'),('sources','ops'),('life','ops'),('gov','ops')],height=7.5)
draw('03-业务数据流','SparkDQ 正式业务数据流',{'a':(1.5,4.5,'1 数据接入\n规范CSV / 源指纹'),'b':(4.5,4.5,'2 冻结任务\n输入 / 规则 / 模板修订'),'c':(7.5,4.5,'3 Spark检测\n完整异常与指标'),'d':(10.5,4.5,'4 事务发布\n结果 / 报告 / 审计'),'e':(10.5,2.3,'5 明确治理策略\n保留原文'),'f':(7.5,2.3,'6 独立数据版本\n父版本与完整指纹'),'g':(4.5,2.3,'7 原规则独立复检\n重新拟合IQR阈值'),'h':(1.5,2.3,'8 历史与报告\n前后对比 / 如实解释')},[('a','b'),('b','c'),('c','d'),('d','e'),('e','f'),('f','g'),('g','h')],height=6)
print('Three diagrams rendered in PNG/SVG/PDF')
