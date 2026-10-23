import sys,json,uuid,subprocess,hashlib,os,csv,io,time,contextlib,fcntl,argparse,signal
from pathlib import Path
r=Path(__file__).resolve().parents[1];sys.path.insert(0,str(r/'infrastructure'))
import storage as st,benchmark as b,native_parquet as n
REASONS={'empty':'at least one record','sentinel':'reserved CSV null marker','nul':'NUL','longcell':'65536','float':'Unsupported Parquet type','timestamp':'Unsupported Parquet type','nested':'Unsupported Parquet type','badname':'unique CSV-compatible names','expanded':'Canonical CSV exceeds20MiB','invalidutf8':'MalformedInputException','old-date':'supported Gregorian range','crlf':'Carriage return cannot round-trip','carriage':'Carriage return cannot round-trip'}
parser=argparse.ArgumentParser(description="Real synthetic Parquet acceptance; no primary writes")
parser.add_argument('--host-capacity',type=Path);parser.add_argument('--image',default='spark-dq-backend:native-parquet');parser.add_argument('--check',type=Path)
args=parser.parse_args()
def interrupted(signum,frame):raise InterruptedError('Synthetic native acceptance interrupted; owned worker evidence retained')
signal.signal(signal.SIGTERM,interrupted)
if args.check:
 root=args.check.absolute();receipt=json.loads((root/'verified.json').read_text());assert receipt['ok']
 n.verify(root/'cases/typed')
 for name,digest in receipt['sourceFiles'].items():assert st.digest(root/'producer/fixtures'/name)==digest
 for key in receipt['rejected']:
  folder=root/'cases'/key;assert not (folder/'COMPLETE').exists() and not (folder/'worker/imported/manifest.json').exists()
  if key=='wrong-pin':assert not folder.exists()
  else:
   log=(folder/'worker/engine.log').read_text(errors='replace');reason='Only unencrypted PAR1 Parquet' if key=='bad-magic' else REASONS[key];assert reason in log
 print(json.dumps({'ok':True,'readonlyCheck':True,'typedRows':6,'rejected':len(receipt['rejected'])}));sys.exit(0)
if not args.host_capacity:parser.error('--host-capacity is required for a fresh run')
capacity=json.loads(args.host_capacity.read_text(encoding='utf-8-sig'));assert capacity.get('known') and capacity.get('freeBytes',0)>5*1024**3
session=uuid.uuid4().hex;root=r/'.runtime'/('native-parquet-'+session[:12]);root.mkdir(mode=0o700)
st.save_json(root/'host-capacity.json',capacity)
with (r/'.runtime/benchmark.lock').open('a') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);marker=r/'.runtime'/('native-pointer-'+session[:12]+'.json');st.save_json(marker,{'folder':str(root)});os.replace(marker,r/'.runtime/native-project.json');before=b.source_snapshot();st.save_json(root/'main-before.json',before)
 main=st.Project();images={svc:st.run(['docker','inspect','--format','{{.Image}}',main.service(svc)]).decode().strip() for svc in ['backend','frontend']}
 st.save_json(root/'main-images-before.json',images)
 image=st.run(['docker','image','inspect','--format','{{.Id}}',args.image]).decode().strip()
 producer=root/'producer';producer.mkdir(mode=0o700);os.chown(producer,1000,1000)
 name='sparkdq-native-fixtures-'+session[:12]
 args=['docker','create','--name',name,'--label','sparkdq.native='+session,'--network','none','--read-only','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','512','--cap-drop','ALL','--security-opt','no-new-privileges','--hostname','sparkdq-worker','--add-host','sparkdq-worker:127.0.0.1','--tmpfs','/tmp:rw,exec,size=768m',
 '--mount','type=bind,src='+str(producer)+',dst=/benchmark/output','--mount','type=bind,src='+str(r/'spark-engine/target/test-classes')+',dst=/fixture-tests,readonly',
 '-e','SPARK_LOCAL_IP=127.0.0.1','--entrypoint','timeout',image,'--signal=TERM','--kill-after=10s','180s','java','-Xmx1g']
 args+=['--add-opens=java.base/'+module+'=ALL-UNNAMED' for module in b.MODULES]
 args+=['-cp','/fixture-tests:/app/engine/spark-engine-0.1.0.jar:/app/engine/dependency/*','cn.sparkdq.engine.NativeParquetFixtures','/benchmark/output/fixtures']
 cid=st.run(args,timeout=60).decode().strip()
 try:
  with (producer/'attach.log').open('wb') as out:st.run(['docker','start','-a',cid],output=out,timeout=200)
 finally:
  assert st.run(['docker','inspect','--format','{{index .Config.Labels "sparkdq.native"}}',cid]).decode().strip()==session
  if st.run(['docker','inspect','--format','{{.State.Running}}',cid]).strip()==b'true':st.run(['docker','stop','--time','10',cid],timeout=30)
  with (producer/'generator.log').open('wb') as out:subprocess.run(['docker','logs',cid],stdout=out,stderr=subprocess.STDOUT,check=True,timeout=30)
 fixtures=producer/'fixtures';typed=fixtures/'typed.parquet';(root/'cases').mkdir()
 good=n.prepare(typed,root/'cases/typed',st.digest(typed)['sha256'],image,capacity)
 expected=[['1','\\N','9.0000000000','2024-02-29','true','2','1','3','中文🙂,\n"quoted"'],['1','Alice','10.0000000000','2024-03-01','false','-2','-1','-3','winner'],
  ['-9223372036854775808','','1234567890123456789012345678.1234567890','1582-10-15','\\N','\\N','\\N','\\N',''],
  ['9223372036854775807','   ','-1.2300000000','9999-12-31','true','32767','127','2147483647',' \n '],
  ['\\N','N','\\N','\\N','false','\\N','\\N','\\N','literal \\N is fine'],['\\N','N2','2.0000000000','2026-01-02','true','0','0','0','high']]
 canonical=root/'cases/typed/worker/imported/canonical.csv'
 with canonical.open(encoding='utf-8',newline='') as stream:actual=list(csv.reader(stream))
 assert actual[0]==['id','name','amount','day','active','small','tiny','number','note'] and sorted(actual[1:])==sorted(expected),(actual,expected)
 st.save_json(root/'expected.json',{'columns':actual[0],'rows':expected});print('PASS actual typed Parquet -> exact canonical scalar values/nulls/Unicode/multiplicity',flush=True)
 failures=[]
 reasons=REASONS
 for key,reason in reasons.items():
  source=fixtures/(key+'.parquet');folder=root/'cases'/key
  try:n.prepare(source,folder,st.digest(source)['sha256'],image,capacity);raise AssertionError('Invalid native input accepted '+key)
  except st.StorageError:
   log=(folder/'worker/engine.log').read_text(errors='replace');assert reason in log,(key,log[-2000:]);assert not (folder/'COMPLETE').exists() and not (folder/'worker/imported/manifest.json').exists()
  failures.append(key);print('PASS fail closed '+key,flush=True)
 for key,source,pin in [('wrong-pin',typed,'0'*64),('bad-magic',fixtures/'bad-magic.parquet',None)]:
  if pin is None:source.write_bytes(b'not a parquet fixture with bad magic');pin=st.digest(source)['sha256']
  folder=root/'cases'/key
  try:n.prepare(source,folder,pin,image,capacity);raise AssertionError('Invalid source accepted')
  except st.StorageError:assert not (folder/'COMPLETE').exists()
  if key=='wrong-pin':assert not folder.exists()
  failures.append(key);print('PASS fail closed '+key,flush=True)
 assert n.verify(root/'cases/typed')['rows']==6
 assert before==b.source_snapshot()
 for svc,image_before in images.items():assert st.run(['docker','inspect','--format','{{.Image}}',main.service(svc)]).decode().strip()==image_before
 result={'ok':True,'folder':str(root),'typedRows':6,'canonical':str(canonical),'positive':good,'rejected':failures,'primaryUnchanged':True,'primaryImagesUnchanged':True,'image':image,'sourceFiles':{f.name:st.digest(f) for f in fixtures.glob('*.parquet')}}
 st.save_json(root/'verified.json',result);print(json.dumps(result,ensure_ascii=False),flush=True)
