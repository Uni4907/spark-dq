from pathlib import Path
import subprocess,time,json,shutil,os,csv
P=Path('/home/sparkdq/projects/spark-dq');root=P/'.runtime'/('t8-hive-'+time.strftime('%Y%m%d-%H%M%S'));root.mkdir(mode=0o755);os.chown(root,185,185)
image='apache/spark@sha256:8cd61978c71aafcd61ac37fcdbbca2ad774396407c9498c79a2afdfc57142783'
assert not subprocess.check_output(['docker','ps','-q'],text=True).strip()
assert shutil.disk_usage(root).free>8*1024**3
mem=int(next(l.split()[1] for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith('MemAvailable:')))*1024;assert mem>4*1024**3
fixture=[['id','n','x','y','note']]
for i in range(14):fixture.append([str(i//2),['1','2','3','4','5','6','999'][i//2],'A' if i<6 else 'B','v' if i<4 else 'w' if i<8 else 'z',['中文','',r'\N','含,逗号','含"引号','内嵌\r\n换行','小数1234567890123456.1234567890'][i//2]])
with (root/'reference.csv').open('w',encoding='utf-8',newline='') as f:csv.writer(f,lineterminator='\r\n',quoting=csv.QUOTE_ALL).writerows(fixture)
shutil.copyfile(Path(__file__).parent/'hive/SparkDQHiveSnapshot.java',root/'SparkDQHiveSnapshot.java')
inventory=subprocess.check_output(['docker','run','--rm','--network','none','--memory','512m','--cpus','1','--entrypoint','sh',image,'-c','ls /opt/spark/jars/*hive* /opt/spark/jars/*derby*'],text=True);(root/'jar-inventory.txt').write_text(inventory)
(root/'environment.json').write_text(json.dumps(dict(image=image,memoryAvailable=mem,quotaBytes=2*1024**3,cpus=2,driverHeap='1g',perProcessSeconds=120,network='none',catalog='hive',metastore='embedded Derby (single process owner)',warehouse='file:/lab/warehouse',sourceCommit=subprocess.check_output(['git','-c','safe.directory='+str(P),'-C',str(P),'rev-parse','HEAD'],text=True).strip()),indent=2))
subprocess.run(['docker','run','--rm','--network','none','--memory','512m','--cpus','1','-v',str(root)+':/lab','-w','/lab','--entrypoint','sh',image,'-c','javac -cp "/opt/spark/jars/*" SparkDQHiveSnapshot.java && jar cf hive-snapshot.jar SparkDQHiveSnapshot.class'],check=True,timeout=60)
for mode in ['create','export']:
 name='sparkdq-t8-'+mode+'-'+time.strftime('%H%M%S')
 cmd=['docker','run','--rm','--name',name,'--network','none','--hostname','hive-lab','--add-host','hive-lab:127.0.0.1','--memory','2g','--cpus','2','-e','SPARK_LOCAL_IP=127.0.0.1','-v',str(root)+':/lab','-w','/lab','--entrypoint','/opt/spark/bin/spark-submit',image,'--driver-memory','1g','--class','SparkDQHiveSnapshot','/lab/hive-snapshot.jar',mode]
 try:
  with (root/(mode+'.log')).open('x') as log:subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,timeout=120,check=True)
 except Exception:
  subprocess.run(['docker','stop','--timeout','5',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=20);print(root,flush=True);raise
print(root,flush=True)

from collections import Counter
with (root/'reference.csv').open(encoding='utf-8',newline='') as f:reference=list(csv.reader(f))
with (root/'hive.csv').open(encoding='utf-8',newline='') as f:actual=list(csv.reader(f))
assert reference[0]==actual[0] and Counter(map(tuple,reference[1:]))==Counter(map(tuple,actual[1:]))
info=json.loads((root/'catalog-evidence.json').read_text());info['fullFixtureMultisetEqual']=True;(root/'catalog-evidence.json').write_text(json.dumps(info,indent=2))
