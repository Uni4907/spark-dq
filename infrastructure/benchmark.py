#!/usr/bin/env python3
"""Bounded, deterministic real-Spark worker benchmark. Never writes production data."""
import argparse,collections,contextlib,csv,datetime as dt,fcntl,hashlib,io,itertools,json,os,platform,random,re,statistics,subprocess,sys,time,uuid,signal
from pathlib import Path
import storage as st
ROOT=st.ROOT
COLUMNS=['id','name','email','amount','score','updated_day','status','note']
PROFILES=('basic','extended','governance')
MODULES=('java.lang','java.lang.invoke','java.lang.reflect','java.io','java.net','java.nio','java.util','java.util.concurrent','java.util.concurrent.atomic','sun.nio.ch','sun.nio.cs','sun.security.action','sun.util.calendar')
SEED=20261019
MAX_INPUT=20*1024**2

def data_rows(count,seed=SEED):
    rng=random.Random(seed)
    for i in range(count):
        name=('\\N' if i%17==0 else '' if i%17==1 else '   ' if i%17==2 else 'N'+str(i%997))
        yield ['k'+str(i//2),name,'\\N' if i%23==0 else 'u'+str(i%97)+'@x.test',
          'bad' if i%29==0 else str(i%1000-100),'9' if i%2==0 else '10',
          '2026-01-01' if i%2==0 else '2026-01-02',
          'pending' if i%31==0 else ' active ' if i%7==0 else 'ACTIVE',f'{rng.getrandbits(32):08x}']

def generate(path,count,seed=SEED):
    if path.exists():raise st.StorageError('Refusing to overwrite a dataset')
    with path.open('x',encoding='utf-8',newline='') as out:
        writer=csv.writer(out,quoting=csv.QUOTE_ALL,lineterminator='\n');writer.writerow(COLUMNS);writer.writerows(data_rows(count,seed))
    if path.stat().st_size>MAX_INPUT:raise st.StorageError('Generated input exceeds application 20 MiB limit; retained, not run')
    os.chmod(path,0o444)
    return dict(rows=count,seed=seed,**st.digest(path))

def rules(profile):
    result=[{'id':'missing-name','type':'MISSING','columns':['name']},
      {'id':'missing-email','type':'MISSING','columns':['email']},
      {'id':'duplicate-id','type':'DUPLICATE','columns':['id']}]
    if profile!='basic':
        result += [{'id':'type-amount','type':'TYPE','columns':['amount'],'params':{'dataType':'INTEGER'}},
          {'id':'range-amount','type':'RANGE','columns':['amount'],'params':{'min':'0','max':'500'}},
          {'id':'regex-status','type':'REGEX','columns':['status'],'params':{'pattern':'ACTIVE'}},
          {'id':'type-day','type':'TYPE','columns':['updated_day'],'params':{'dataType':'DATE'}}]
    return result

def policy():
    return {'policyVersion':4,'dropMissing':True,'deduplicate':True,
      'normalizations':{'status':'TRIM_UPPER'},'mappings':{'status':[{'from':'PENDING','to':'ACTIVE'}]},
      'fills':{'name':'UNKNOWN','email':'unknown@x.test'},
      'deduplicationOrder':[{'column':'score','type':'DECIMAL','direction':'DESC','nulls':'LAST'},
       {'column':'updated_day','type':'DATE','direction':'DESC','nulls':'LAST'}]}

def missing(value):return value=='\\N' or value.strip(' ')==''
def governed_rows(count,seed):
    # Independent oracle: each two-row key has scores 9 then 10, so odd row wins.
    for i,row in enumerate(data_rows(count,seed)):
        if i%2==0 and i!=count-1:continue
        row[1]='UNKNOWN' if missing(row[1]) else row[1]
        row[2]='unknown@x.test' if missing(row[2]) else row[2]
        row[6]='ACTIVE'
        yield row

def row_digest(rows):
    # CSV order is not guaranteed; compare a sorted canonical multiset, including multiplicity.
    lines=sorted(json.dumps(row,ensure_ascii=False,separators=(',',':')).encode() for row in rows)
    sha=hashlib.sha256()
    for line in lines:sha.update(line+b'\n')
    return sha.hexdigest()

def oracle(count,profile,seed=SEED):
    selected=governed_rows(count,seed) if profile=='governance' else data_rows(count,seed)
    counts=collections.Counter();keys=collections.Counter();total=0
    for row in selected:
        total+=1;keys[row[0]]+=1
        for index,name in ((1,'name'),(2,'email')):
            value=row[index]
            counts[name+'-null']+=value=='\\N'
            counts[name+'-empty']+=value==''
            counts[name+'-spaces']+=value!='' and value!='\\N' and value.strip(' ')==''
        try:value=int(row[3]);invalid_number=False
        except ValueError:value=0;invalid_number=True
        counts['bad-amount']+=invalid_number
        counts['bad-range']+=invalid_number or not 0<=value<=500
        counts['bad-status']+=row[6]!='ACTIVE'
    expected={}
    for name in ('name','email'):
        nulls,empty,spaces=(counts[name+'-'+x] for x in ('null','empty','spaces'))
        expected['missing-'+name]={'nullCount':nulls,'emptyCount':empty,'whitespaceCount':spaces,'blankCount':nulls+empty+spaces}
    duplicate=[n for n in keys.values() if n>1]
    expected['duplicate-id']={'duplicateGroups':len(duplicate),'duplicateRows':sum(duplicate),'excessRows':sum(n-1 for n in duplicate)}
    if profile!='basic':
        base={'checkedCount':total,'skippedBlankCount':0}
        expected.update({'type-amount':dict(base,invalidCount=counts['bad-amount']),
          'range-amount':dict(base,invalidCount=counts['bad-range'],nonNumericCount=counts['bad-amount']),
          'regex-status':dict(base,invalidCount=counts['bad-status']),
          'type-day':dict(base,invalidCount=0)})
    result={'totalRows':total,'metrics':expected}
    if profile=='governance':
        result['outputRowsSha256']=row_digest(governed_rows(count,seed))
        before=list(data_rows(count,seed))
        pending=sum(row[6]=='pending' for row in before)
        result['audit']={'inputRows':count,'outputRows':total,'removedRows':count-total,'deduplicatedRows':count-total,
          'droppedMissingRows':0,'policyVersion':4,
          'transformations':[{'column':'status','operation':'TRIM_UPPER','changedRows':sum(row[6]!=row[6].strip(' ').upper() for row in before)},
           {'column':'status','operation':'VALUE_MAPPING','matchedRows':pending,'changedRows':pending,
            'entries':[{'from':'PENDING','to':'ACTIVE','matchedRows':pending,'changedRows':pending}]},
           {'column':'email','operation':'FILL_MISSING','changedRows':sum(missing(row[2]) for row in before)},
           {'column':'name','operation':'FILL_MISSING','changedRows':sum(missing(row[1]) for row in before)}],
          'deduplication':{'inputRows':count,'duplicateGroups':count//2,'order':policy()['deduplicationOrder'],
           'tieBreak':'FULL_ROW_LEXICOGRAPHIC_ASC_NULLS_FIRST'}}
    return result

def validate_result(folder,expected,partitions):
    result=json.loads((folder/'result/result.json').read_text())
    assert result['totalRows']==expected['totalRows']
    actual={item['ruleId']:item['metrics'] for item in result['results']}
    assert actual==expected['metrics'],('metric mismatch',actual,expected['metrics'])
    for rule in result['results']:
        metrics=rule['metrics'];required=metrics.get('blankCount',metrics.get('duplicateRows',metrics.get('invalidCount')))
        count=0
        for path in (folder/'result/anomalies'/rule['ruleId']).glob('part-*.json'):
            with path.open(encoding='utf-8') as stream:
                for line in stream:
                    row=json.loads(line)
                    fields=set(COLUMNS)|({'__dq_group_size'} if rule['ruleId']=='duplicate-id' else set())
                    assert set(row)==fields,('unexpected anomaly schema',rule['ruleId'],sorted(row))
                    if rule['ruleId']=='duplicate-id':assert row['__dq_group_size']==2
                    count+=1
        assert count==required,('anomaly count mismatch',rule['ruleId'],count,required)
    if 'audit' in expected:
        assert result['governance']==expected['audit'],('audit mismatch',result['governance'],expected['audit'])
        parts=list((folder/'result/governed').glob('part-*.csv'));assert len(parts)==1
        with parts[0].open(encoding='utf-8-sig',newline='') as stream:
            reader=csv.reader(stream);assert next(reader)==COLUMNS
            assert row_digest(reader)==expected['outputRowsSha256'],'governed values differ from independent oracle'
    execution=result['execution']
    assert execution['inputPartitions']==partitions and execution['shufflePartitions']==2
    assert execution['sparkMaster']=='local[2]'
    for key in ('workerElapsedMs','workerCpuMs','peakRssBytes','maxHeapBytes','inputReadAndCountMs','rulesAndAnomalyWriteMs'):
        assert isinstance(execution.get(key),int) and execution[key]>0,(key,execution.get(key))
    return result

def plan(rows,partitions,repeats,warmups,seed,profiles=PROFILES):
    if not rows or len(set(rows))!=len(rows) or any(type(n) is not int or not 100<=n<=250000 for n in rows):raise ValueError('Rows must be distinct integers in 100..250000')
    if not partitions or len(set(partitions))!=len(partitions) or any(type(n) is not int or not 1<=n<=16 for n in partitions):raise ValueError('Partitions must be distinct integers in 1..16')
    if repeats not in (1,2,3) or warmups not in (0,1,2) or not set(profiles).issubset(PROFILES) or len(set(profiles))!=len(profiles) or not profiles:raise ValueError('Invalid repeats/warmups/profiles')
    trials=[dict(rows=n,partitions=p,profile=profile,repeat=repeat,warmup=False) for n,p,profile,repeat in itertools.product(rows,partitions,profiles,range(1,repeats+1))]
    random.Random(seed).shuffle(trials)
    warm=[dict(rows=min(rows),partitions=p,profile='extended',repeat=i,warmup=True) for p,i in itertools.product(partitions,range(1,warmups+1))]
    if len(trials)+len(warm)>100:raise ValueError('At most 100 bounded trials per run')
    for index,trial in enumerate(warm+trials,1):trial['index']=index
    return warm+trials

def source_idle():
    for status in ('QUEUED','RUNNING','CANCELLING'):
        with st.LOCAL.open('http://127.0.0.1:18080/api/tasks/page?pageSize=1&status='+status,timeout=5) as response:data=json.load(response)
        if data['total']:raise st.StorageError('Source has active user work; benchmark stopped without cancelling it')

def source_snapshot():
    project=st.Project();source_idle()
    rows={table:hashlib.sha256(project.mysql('SELECT * FROM '+table+' ORDER BY 1,2;')).hexdigest() for table in st.TABLES}
    files=st.scan_tar(io.BytesIO(st.run(st.tar_command(project.volume('backend','/data')),timeout=120)))
    return {'businessRows':rows,'files':files}

def aggregate(records):
    groups=collections.defaultdict(list)
    for row in records:
        if row['warmup'] or row['status']!='verified':continue
        groups[(row['rows'],row['partitions'],row['profile'])].append(row)
    result=[]
    for (n,p,profile),items in sorted(groups.items()):
        wall=[item['containerWallSeconds'] for item in items]
        result.append({'rows':n,'partitions':p,'profile':profile,'repeats':len(items),
          'medianWallSeconds':statistics.median(wall),'minWallSeconds':min(wall),'maxWallSeconds':max(wall),
          'inputRowsPerSecond':n/statistics.median(wall),
          'medianWorkerCpuSeconds':statistics.median(item['execution']['workerCpuMs']/1000 for item in items),
          'medianPeakRssMiB':statistics.median(item['execution']['peakRssBytes']/1024**2 for item in items),
          'medianReadAndCountSeconds':statistics.median(item['execution']['inputReadAndCountMs']/1000 for item in items),
          'medianRulesAndAnomalySeconds':statistics.median(item['execution']['rulesAndAnomalyWriteMs']/1000 for item in items)})
    return result

def trial(output,trial,image,session,expected,timeout=300):
    folder=output/'cases'/f"{trial['index']:03d}-{trial['profile']}-{trial['rows']}-p{trial['partitions']}"
    folder.mkdir();os.chown(folder,1000,1000);os.chmod(folder,0o700)
    payload={'taskId':folder.name,'input':'/benchmark/input/'+str(trial['rows'])+'.csv',
      'output':'/benchmark/output/result','semanticsVersion':'csv-v2','rules':rules(trial['profile'])}
    if trial['profile']=='governance':payload['governance']=policy()
    st.save_json(folder/'request.json',payload);os.chown(folder/'request.json',1000,1000)
    name='sparkdq-bench-'+session[:12]+'-'+str(trial['index'])
    args=['docker','create','--name',name,'--label','sparkdq.benchmark='+session,
      '--network','none','--read-only','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','512',
      '--cap-drop','ALL','--security-opt','no-new-privileges','--tmpfs','/tmp:rw,size=768m',
      '--mount','type=bind,src='+str(output/'datasets')+',dst=/benchmark/input,readonly',
      '--mount','type=bind,src='+str(folder)+',dst=/benchmark/output',
      '-e','SPARK_LOCAL_IP=127.0.0.1','-e','SPARK_MASTER=local[2]',
      '-e','SPARK_INPUT_PARTITIONS='+str(trial['partitions']),'-e','SPARK_SHUFFLE_PARTITIONS=2',
      '--entrypoint','timeout',image,'--signal=TERM','--kill-after=10s',str(timeout)+'s','java','-Xmx1g']
    args+=['--add-opens=java.base/'+module+'=ALL-UNNAMED' for module in MODULES]
    args+=['-cp','/app/engine/spark-engine-0.1.0.jar:/app/engine/dependency/*','cn.sparkdq.engine.Engine','/benchmark/output/request.json']
    cid=st.run(args,timeout=60).decode().strip()
    record=dict(trial,containerId=cid,status='started')
    st.save_json(folder/'started.json',record)
    started=time.perf_counter()
    try:
        with (folder/'engine.log').open('wb') as log:st.run(['docker','start','-a',cid],output=log,timeout=timeout+15)
        record['containerWallSeconds']=time.perf_counter()-started
        state=json.loads(st.run(['docker','inspect','--format','{{json .State}}',cid]))
        if state['Running'] or state['ExitCode']!=0 or state['OOMKilled']:raise st.StorageError('Worker failed, timed out or was OOM killed; evidence retained')
        value=validate_result(folder,expected,trial['partitions'])
        record.update(status='verified',execution=value['execution'],totalRows=value['totalRows'])
        source_idle()
    finally:
        # Stop only this newly created, label-verified container, including start/timeout failures.
        label=st.run(['docker','inspect','--format','{{index .Config.Labels "sparkdq.benchmark"}}',cid]).decode().strip()
        if label!=session:raise st.StorageError('Container ownership mismatch; no stop attempted')
        if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':
            st.run(['docker','stop','--time','10',cid],timeout=30)
    st.save_json(folder/'measurement.json',record)
    return record

def save_report(output,summary):
    with (output/'measurements.csv').open('x',encoding='utf-8',newline='') as stream:
        fields=['index','rows','partitions','profile','repeat','warmup','containerWallSeconds','totalRows','status','workerElapsedMs','workerCpuMs','peakRssBytes','inputReadAndCountMs','governanceAndOutputCountMs','rulesAndAnomalyWriteMs']
        writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader()
        for item in summary['records']:writer.writerow({key:item.get(key,item['execution'].get(key,'')) for key in fields})
    with (output/'summary.csv').open('x',encoding='utf-8',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(summary['groups'][0]));writer.writeheader();writer.writerows(summary['groups'])
    st.save_json(output/'summary.json',summary)
    lines=['# SparkDQ 单机实测基准','',
      '固定 local[2]、2 CPU 配额、2 GiB 容器内存、1 GiB JVM堆；只改变输入分区1/2/4（以实际计划为准），shuffle固定2、AQE保留默认并逐次记录。',
      '新JVM逐次启动；计时为容器start到退出，包含Spark初始化、输入读取/重分区、规则计算和全部异常文件落盘，治理组还包括新文件保存与真实文件复检；不含上传、队列、MySQL、容器create及Python核对。',
      '预热仍使用独立JVM，仅影响宿主缓存；不清空宿主页缓存。RSS是/proc/self/status的VmHWM，不是堆内存或容器总内存。CPU是JVM进程累计CPU时间，RSS和CPU均在结果生成时观测，不含之后的关闭阶段。两次重复只报告中位数和范围，不作显著性推断。',
      '所有试次均核对独立预期指标、异常文件条数及治理CSV多重集和审计；主库业务行/文件前后相同。仅本机小于20MiB样本，不构成集群或TB级性能证据。','',
      '| 行数 | 输入分区 | 流程 | 重复 | 中位总秒 | 最小～最大秒 | 输入行/秒 | 峰值RSS MiB |',
      '| --- | --- | --- | --- | --- | --- | --- | --- |']
    for group in summary['groups']:
        lines.append(f"| {group['rows']} | {group['partitions']} | {group['profile']} | {group['repeats']} | {group['medianWallSeconds']:.3f} | {group['minWallSeconds']:.3f}～{group['maxWallSeconds']:.3f} | {group['inputRowsPerSecond']:.0f} | {group['medianPeakRssMiB']:.1f} |")
    (output/'report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')

def run(args):
    deadline=time.monotonic()+args.max_seconds
    trials=plan(args.rows,args.partitions,args.repeats,args.warmups,args.seed,args.profiles)
    runtime=(ROOT/'.runtime').resolve();output=args.output.absolute()
    output.resolve().relative_to(runtime)
    if output.exists():raise st.StorageError('Refusing to reuse an existing benchmark directory')
    if not output.parent.is_dir():raise st.StorageError('Output parent must exist')
    if args.host_capacity:
        capacity=json.loads(args.host_capacity.read_text(encoding='utf-8-sig'))
        if not capacity.get('known') or capacity.get('freeBytes',0)<5*1024**3:raise st.StorageError('Host backing drive needs a known 5 GiB free reserve')
    else:capacity={'known':False,'warning':'Linux virtual free space does not establish Windows backing-drive capacity'}
    if __import__('shutil').disk_usage(output.parent).free<5*1024**3:raise st.StorageError('Insufficient Linux space')
    source_idle();image=st.run(['docker','image','inspect','--format','{{.Id}}',args.image]).decode().strip()
    output.mkdir(mode=0o700);(output/'datasets').mkdir(mode=0o755);(output/'cases').mkdir(mode=0o700)
    session=uuid.uuid4().hex
    environment={'seed':args.seed,'orderingSeed':args.seed,'master':'local[2]','cpuQuota':2,'memoryLimitBytes':2*1024**3,'jvmHeap':'1g',
      'shufflePartitions':2,'image':image,'kernel':platform.platform(),'pythonVersion':platform.python_version(),'docker':st.run(['docker','info','--format','{{json .}}']).decode()}
    # Keep only selected non-sensitive engine facts, never full Docker info/environment.
    info=json.loads(environment.pop('docker'));environment['docker']={key:info.get(key) for key in ('ServerVersion','NCPU','MemTotal','CgroupVersion','CgroupDriver')}
    environment['hostCapacity']=capacity
    environment['sourceBaseCommit']=st.run(['git','-c','safe.directory='+str(ROOT),'rev-parse','HEAD']).decode().strip()
    source_files=['spark-engine/src/main/java/cn/sparkdq/engine/Engine.java','spark-engine/src/main/java/cn/sparkdq/engine/WorkerMetrics.java','infrastructure/benchmark.py']
    environment['sourceDigests']={name:st.digest(ROOT/name) for name in source_files}
    environment['engineJarSha256']=st.run(['docker','run','--rm','--network','none','--entrypoint','sha256sum',image,'/app/engine/spark-engine-0.1.0.jar']).decode().split()[0]
    st.save_json(output/'environment.json',environment);st.save_json(output/'plan.json',trials)
    datasets={str(n):generate(output/'datasets'/f'{n}.csv',n,args.seed) for n in args.rows}
    st.save_json(output/'datasets.json',datasets)
    expected={(n,profile):oracle(n,profile,args.seed) for n in args.rows for profile in set(args.profiles)|{'extended'}}
    st.save_json(output/'oracle.json',{str(n)+'/'+profile:value for (n,profile),value in expected.items()})
    before=source_snapshot();st.save_json(output/'source-before.json',before);records=[]
    for item in trials:
        remaining=int(deadline-time.monotonic())
        if remaining<20:raise st.StorageError('Global benchmark deadline reached; partial evidence retained')
        source_idle();record=trial(output,item,image,session,expected[(item['rows'],item['profile'])],timeout=min(300,remaining));records.append(record)
        print(json.dumps({'completed':len(records),'total':len(trials),'rows':item['rows'],'partitions':item['partitions'],'profile':item['profile'],'warmup':item['warmup'],'seconds':round(record['containerWallSeconds'],3),'verified':True}),flush=True)
    after=source_snapshot();st.save_json(output/'source-after.json',after)
    for name,digest in environment['sourceDigests'].items():
        if st.digest(ROOT/name)!=digest:raise st.StorageError('Experiment source changed during execution; no complete conclusion emitted')
    if before!=after:raise st.StorageError('Source changed during experiments; no complete performance conclusion emitted')
    for n,meta in datasets.items():
        if st.digest(output/'datasets'/f'{n}.csv')!={key:meta[key] for key in ('sha256','size')}:raise st.StorageError('Input changed during experiments')
    summary={'formatVersion':'sparkdq-benchmark-v1','complete':True,'finishedAt':dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(),
      'sourceUnchanged':True,'warmupTrials':sum(item['warmup'] for item in records),'measuredTrials':sum(not item['warmup'] for item in records),
      'environment':environment,'datasets':datasets,'groups':aggregate(records),'records':records}
    save_report(output,summary);st.save_json(output/'COMPLETE',{'summarySha256':st.digest(output/'summary.json')['sha256']})
    print(json.dumps({'complete':True,'output':str(output),'measuredTrials':summary['measuredTrials'],'groups':len(summary['groups'])}),flush=True)

def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('run');p.add_argument('--output',required=True,type=Path);p.add_argument('--image',default='spark-dq-backend:benchmark')
    p.add_argument('--rows',nargs='+',type=int,default=[10000,50000,200000]);p.add_argument('--partitions',nargs='+',type=int,default=[1,2,4])
    p.add_argument('--profiles',nargs='+',choices=PROFILES,default=list(PROFILES));p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--warmups',type=int,default=1);p.add_argument('--seed',type=int,default=SEED);p.add_argument('--host-capacity',type=Path)
    p.add_argument('--max-seconds',type=int,default=1800)
    args=parser.parse_args()
    if not 30<=args.max_seconds<=3600:parser.error('max-seconds must be in 30..3600')
    def interrupted(signum,frame):raise InterruptedError('Benchmark interrupted; owned worker will be stopped')
    signal.signal(signal.SIGTERM,interrupted)
    try:
        with (ROOT/'.runtime/benchmark.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);run(args)
        return 0
    except (Exception,KeyboardInterrupt) as error:
        print(json.dumps({'complete':False,'errorType':type(error).__name__,'message':str(error)[:1500],'note':'Partial evidence retained; user work was not cancelled'}),file=sys.stderr)
        return 1
if __name__=='__main__':sys.exit(main())
