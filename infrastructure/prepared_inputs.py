#!/usr/bin/env python3
"""Offline prepared-format acceptance and bounded comparison. No HTTP format rollout."""
import argparse,collections,contextlib,csv,fcntl,json,os,random,shutil,signal,statistics,sys,time,uuid
from pathlib import Path
import benchmark as b
import storage as st

def worker(folder,input_root,payload,image,session,prepare=False,timeout=300,import_parquet=False):
    if prepare and import_parquet:raise st.StorageError("Choose one worker operation")
    folder.mkdir(mode=0o700);os.chown(folder,1000,1000)
    st.save_json(folder/'request.json',payload);os.chown(folder/'request.json',1000,1000)
    name='sparkdq-format-'+uuid.uuid4().hex[:16]
    command=['docker','create','--name',name,'--label','sparkdq.prepared='+session,'--network','none','--read-only',
      '--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','512','--cap-drop','ALL',
      '--security-opt','no-new-privileges','--hostname','sparkdq-worker','--add-host','sparkdq-worker:127.0.0.1',
      '--tmpfs','/tmp:rw,exec,size=768m',
      '--mount','type=bind,src='+str(input_root)+',dst=/benchmark/input,readonly',
      '--mount','type=bind,src='+str(folder)+',dst=/benchmark/output',
      '-e','SPARK_LOCAL_IP=127.0.0.1','-e','SPARK_MASTER=local[2]','--entrypoint','timeout',
      image,'--signal=TERM','--kill-after=10s',str(timeout)+'s','java','-Xmx1g']
    command+=['--add-opens=java.base/'+module+'=ALL-UNNAMED' for module in b.MODULES]
    command+=['-cp','/app/engine/spark-engine-0.1.0.jar:/app/engine/dependency/*',
      'cn.sparkdq.engine.'+('ParquetImport' if import_parquet else 'PreparedInput' if prepare else 'Engine'),'/benchmark/output/request.json']
    cid=st.run(command,timeout=60).decode().strip();st.save_json(folder/'started.json',{'containerId':cid,'session':session})
    start=time.perf_counter()
    try:
        with (folder/'attach.log').open('wb') as log:st.run(['docker','start','-a',cid],output=log,timeout=timeout+15)
        wall=time.perf_counter()-start
        state=json.loads(st.run(['docker','inspect','--format','{{json .State}}',cid]))
        if state['ExitCode'] or state['Running'] or state['OOMKilled']:raise st.StorageError('Prepared worker failed; log retained')
        return {'wallSeconds':wall,'containerId':cid}
    finally:
        label=st.run(['docker','inspect','--format','{{index .Config.Labels "sparkdq.prepared"}}',cid]).decode().strip()
        if label!=session:raise st.StorageError('Ownership mismatch; no container stop attempted')
        if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':
            st.run(['docker','stop','--time','10',cid],timeout=30)
        # Attach stderr can be separate; Docker logs merges both streams and retains Spark failures.
        import subprocess
        with (folder/'engine.log').open('xb') as log:
            subprocess.run(['docker','logs',cid],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=30)

def edge_rows():
    rows=list(b.data_rows(20))
    notes=['中文🙂','line one\nline two','CR\r\nLF','comma,quote"','literal \\N and \\ slash',' leading and trailing ']
    for i,row in enumerate(rows):row[-1]=notes[i%len(notes)]
    return rows

def anomaly_hash(folder):
    hashes={}
    for rule in sorted((folder/'result/anomalies').iterdir()):
        rows=[]
        for file in rule.glob('part-*.json'):
            rows.extend(json.loads(line) for line in file.read_text(encoding='utf-8').splitlines())
        hashes[rule.name]=b.row_digest([[row.get(name) for name in b.COLUMNS]+([row['__dq_group_size']] if '__dq_group_size' in row else []) for row in rows])
    return hashes

def summarize(records,preparations):
    result=[]
    for rows,profile,kind in sorted({(x['rows'],x['profile'],x['format']) for x in records if not x['edge']}):
        selected=[x for x in records if (x['rows'],x['profile'],x['format'])==(rows,profile,kind) and not x['edge']]
        walls=[x['wallSeconds'] for x in selected]
        prep=preparations.get(str(rows)+'/'+kind,{}).get('wallSeconds',0)
        result.append({'rows':rows,'profile':profile,'format':kind,'repeats':len(selected),
          'medianWallSeconds':statistics.median(walls),'minWallSeconds':min(walls),'maxWallSeconds':max(walls),
          'medianReadSeconds':statistics.median(x['execution']['inputReadAndCountMs']/1000 for x in selected),
          'actualPartitions':sorted({x['execution']['inputPartitions'] for x in selected}),
          'preparationWallSeconds':prep,'preparePlusOneRunSeconds':prep+statistics.median(walls)})
    return result

def run(args):
    b.plan(args.rows,[1],args.repeats,0,b.SEED)
    output=args.output.absolute();output.resolve().relative_to(st.ROOT/'.runtime')
    if output.exists() or not output.parent.is_dir():raise st.StorageError('Output must be a new runtime directory')
    capacity=json.loads(args.host_capacity.read_text(encoding='utf-8-sig'))
    if not capacity.get('known') or capacity.get('freeBytes',0)<5*1024**3 or shutil.disk_usage(output.parent).free<5*1024**3:
        raise st.StorageError('A known 5 GiB host and Linux free reserve is required')
    deadline=time.monotonic()+args.max_seconds;session=uuid.uuid4().hex
    def limit():
        remaining=int(deadline-time.monotonic())
        if remaining<10:raise st.StorageError('Global deadline reached; partial evidence retained')
        return min(300,remaining)
    b.source_idle();before=b.source_snapshot()
    output.mkdir(mode=0o700);inputs=output/'inputs';inputs.mkdir(mode=0o755);(output/'cases').mkdir()
    image=st.run(['docker','image','inspect','--format','{{.Id}}',args.image]).decode().strip()
    files=['spark-engine/src/main/java/cn/sparkdq/engine/'+name for name in ('Engine.java','PreparedInput.java','WorkerMetrics.java')]
    files+=['infrastructure/prepared_inputs.py']
    environment={'image':image,'sourceBaseCommit':st.run(['git','-c','safe.directory='+str(st.ROOT),'rev-parse','HEAD']).decode().strip(),
      'sourceDigests':{name:st.digest(st.ROOT/name) for name in files},'hostCapacity':capacity,'master':'local[2]','cpuQuota':2,
      'containerMemoryBytes':2*1024**3,'heap':'1g','shufflePartitions':2,'requestedPartitions':0,
      'engineJarSha256':st.run(['docker','run','--rm','--network','none','--entrypoint','sha256sum',image,'/app/engine/spark-engine-0.1.0.jar']).decode().split()[0]}
    st.save_json(output/'environment.json',environment);st.save_json(output/'source-before.json',before)
    datasets={}
    for n in args.rows:datasets[str(n)]=b.generate(inputs/(str(n)+'.csv'),n)
    edge=inputs/'edge.csv'
    with edge.open('x',encoding='utf-8',newline='') as stream:
        writer=csv.writer(stream,quoting=csv.QUOTE_ALL,lineterminator='\n');writer.writerow(b.COLUMNS);writer.writerows(edge_rows())
    os.chmod(edge,0o444);datasets['edge']=dict(rows=20,**st.digest(edge))
    preparations={}
    for key in datasets:
        for kind in ('CSV','PARQUET'):
            b.source_idle();folder=output/'cases'/('prepare-'+key+'-'+kind.lower())
            payload={'input':'/benchmark/input/'+key+'.csv','output':'/benchmark/output/prepared','format':kind,'partitions':4}
            measured=worker(folder,inputs,payload,image,session,prepare=True,timeout=limit())
            proof=json.loads((folder/'prepared/preparation.json').read_text());assert proof['verified'] and proof['rows']==datasets[key]['rows']
            destination=inputs/(key+'-'+kind.lower());shutil.copytree(folder/'prepared',destination)
            for file in destination.rglob('*'):
                if file.is_file():os.chmod(file,0o444)
            preparations[key+'/'+kind]=dict(measured,**proof,preparedBytes=sum(p.stat().st_size for p in (destination/'data').glob('part-*')))
            print('PASS preparation '+key+'/'+kind,flush=True)
    jobs=[(key,kind,profile,repeat) for key in datasets for kind in ('CSV_SINGLE','CSV','PARQUET')
      for profile in ('extended','governance') for repeat in range(1,(1 if key=='edge' else args.repeats)+1)]
    random.Random(b.SEED).shuffle(jobs);st.save_json(output/'plan.json',jobs)
    records=[];anomalies={}
    for index,(key,kind,profile,repeat) in enumerate(jobs):
        b.source_idle();n=datasets[key]['rows'];folder=output/'cases'/f'{index:03d}-{key}-{kind}-{profile}-{repeat}'
        payload={'taskId':folder.name,'output':'/benchmark/output/result','semanticsVersion':'csv-v2','rules':b.rules(profile)}
        if profile=='governance':payload['governance']=b.policy()
        if kind=='CSV_SINGLE':payload['input']='/benchmark/input/'+key+'.csv'
        else:
            manifest=inputs/(key+'-'+kind.lower())/'manifest.json'
            pinned=preparations[key+'/'+kind]['manifestSha256']
            assert st.digest(manifest)['sha256']==pinned,'Prepared manifest changed after readback'
            payload.update(inputManifest='/benchmark/input/'+key+'-'+kind.lower()+'/manifest.json',inputManifestSha256=pinned)
        measured=worker(folder,inputs,payload,image,session,timeout=limit())
        actual=json.loads((folder/'result/result.json').read_text());expected=b.oracle(n,profile)
        if key=='edge' and profile=='governance':
            governed=list(b.governed_rows(20,b.SEED))
            for j,row in enumerate(governed):row[-1]=edge_rows()[2*j+1][-1]
            expected['outputRowsSha256']=b.row_digest(governed)
        b.validate_result(folder,expected,actual['execution']['inputPartitions'])
        assert actual['execution']['requestedInputPartitions']==0 and actual['execution']['inputFormat']==kind
        hashes=anomaly_hash(folder);group=key+'/'+profile
        if group in anomalies:assert hashes==anomalies[group],'Anomaly row multiset differs across formats'
        else:anomalies[group]=hashes
        record=dict(measured,index=index,rows=n,edge=key=='edge',format=kind,profile=profile,repeat=repeat,execution=actual['execution'],anomalyDigests=hashes)
        records.append(record);st.save_json(folder/'measurement.json',record)
        print(json.dumps({'completed':len(records),'total':len(jobs),'dataset':key,'format':kind,'profile':profile,'seconds':round(measured['wallSeconds'],3),'verified':True}),flush=True)
    # Real failures must produce no result, including a re-signed manifest with false row count.
    failures=[]
    for kind in ('pin','data','rows','path','header','reuse'):
        # Copy listed data without Hadoop's optional CRC sidecars, so a deliberately
        # re-signed bad header reaches the header validator instead of a stale CRC.
        fixture=inputs/('fault-'+kind)
        shutil.copytree(inputs/'edge-csv',fixture,ignore=shutil.ignore_patterns('.*.crc'))
        manifest=fixture/'manifest.json';value=json.loads(manifest.read_text());target=fixture/value['files'][0]['path']
        if kind=='data':os.chmod(target,0o644);target.write_bytes(target.read_bytes()+b'X')
        if kind=='rows':value['rowCount']+=1
        if kind=='path':value['files'][0]['path']='../edge.csv'
        if kind=='header':
            os.chmod(target,0o644);target.write_bytes(target.read_bytes().replace(b'"id"',b'"ID"',1))
            value['files'][0].update(st.digest(target))
        if kind in ('rows','path','header'):
            os.chmod(manifest,0o644);manifest.write_text(json.dumps(value),encoding='utf-8')
        folder=output/'cases'/('fault-'+kind)
        payload={'taskId':'fault','inputManifest':'/benchmark/input/'+fixture.name+'/manifest.json',
          'inputManifestSha256':('a'*64 if kind=='pin' else st.digest(manifest)['sha256']),
          'output':'/benchmark/output/result','semanticsVersion':'csv-v2','rules':b.rules('extended')}
        if kind=='reuse':
            # Existing directory refusal is exercised by trying to prepare directly over the mounted input.
            payload={'input':'/benchmark/input/edge.csv','output':'/benchmark/input/edge-csv','format':'CSV','partitions':4}
        try:worker(folder,inputs,payload,image,session,prepare=kind=='reuse',timeout=limit())
        except st.StorageError:
            started=json.loads((folder/'started.json').read_text())
            state=json.loads(st.run(['docker','inspect','--format','{{json .State}}',started['containerId']]))
            expected_error={'pin':'Prepared manifest checksum mismatch','data':'Prepared file size mismatch',
              'rows':'Prepared row count mismatch','path':'Invalid prepared file path','header':'Prepared CSV header mismatch',
              'reuse':'Preparation output must be new'}[kind]
            assert state['ExitCode']==1 and not state['Running'] and not state['OOMKilled']
            assert expected_error in (folder/'engine.log').read_text()
            assert not (folder/'result/result.json').exists();failures.append(kind)
        else:raise AssertionError('Failure probe unexpectedly succeeded: '+kind)
    after=b.source_snapshot();st.save_json(output/'source-after.json',after);assert before==after
    for key,value in datasets.items():assert st.digest(inputs/(key+'.csv'))=={k:value[k] for k in ('sha256','size')}
    for key,proof in preparations.items():
        dataset,kind=key.split('/');directory=inputs/(dataset+'-'+kind.lower());manifest=directory/'manifest.json'
        assert st.digest(manifest)['sha256']==proof['manifestSha256']
        for file in json.loads(manifest.read_text())['files']:
            assert st.digest(directory/file['path'])=={k:file[k] for k in ('sha256','size')}
    for name,value in environment['sourceDigests'].items():assert st.digest(st.ROOT/name)==value
    summary={'complete':True,'formatVersion':'sparkdq-prepared-comparison-v1','environment':environment,
      'sourceUnchanged':True,'datasets':datasets,'preparations':preparations,'records':records,
      'groups':summarize(records,preparations),'faultsVerified':failures,'measuredTrials':sum(not x['edge'] for x in records),'edgeTrials':sum(x['edge'] for x in records)}
    st.save_json(output/'summary.json',summary);st.save_json(output/'COMPLETE',{'summarySha256':st.digest(output/'summary.json')['sha256']})
    print(json.dumps({'complete':True,'output':str(output),'measuredTrials':summary['measuredTrials'],'edgeTrials':summary['edgeTrials'],'faultsVerified':failures}),flush=True)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--image',default='spark-dq-backend:prepared')
    p.add_argument('--rows',nargs='+',type=int,default=[10000,200000]);p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--host-capacity',type=Path,required=True);p.add_argument('--max-seconds',type=int,default=1800);args=p.parse_args()
    if not 30<=args.max_seconds<=3600:p.error('max-seconds must be 30..3600')
    def interrupted(signum,frame):raise InterruptedError('Prepared input comparison interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    try:
        with (st.ROOT/'.runtime/benchmark.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);run(args)
        return 0
    except (Exception,KeyboardInterrupt) as error:
        print(json.dumps({'complete':False,'errorType':type(error).__name__,'message':str(error)[:1500],'note':'Partial evidence retained'}),file=sys.stderr);return 1
if __name__=='__main__':sys.exit(main())
