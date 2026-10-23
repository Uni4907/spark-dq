from pathlib import Path
import subprocess,secrets,json,time,sys,os,traceback,xml.etree.ElementTree as ET
P=Path('/home/sparkdq/projects/spark-dq');root=sorted((P/'.runtime').glob('t10-ui-*'))[-1];meta=json.loads((root/'lab.json').read_text());out=root/'jdbc-integration';out.mkdir(mode=0o700);identity='sparkdq-t10-jdbc-'+secrets.token_hex(4);password=secrets.token_hex(24);reader=secrets.token_hex(24)
env=out/'private.env';env.write_text('MYSQL_ROOT_PASSWORD='+password+'\nMYSQL_ROOT_HOST=%\nSNAPSHOT_TEST_ROOT_PASSWORD='+password+'\nSNAPSHOT_TEST_READER_PASSWORD='+reader+'\nSNAPSHOT_TEST_URL=jdbc:mysql://jdbc-fixture:3306/?allowPublicKeyRetrieval=true&useSSL=false&characterEncoding=UTF-8\n');env.chmod(0o600)
network=meta['project']+'_default';volume=identity+'-data';result=dict(ok=False);cid=None
try:
 assert int(next(l.split()[1] for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith('MemAvailable:')))*1024>4*1024**3
 subprocess.run(['docker','volume','create','--label','sparkdq.purpose=t10-jdbc-fixture',volume],stdout=subprocess.DEVNULL,check=True,timeout=20)
 cid=subprocess.check_output(['docker','run','-d','--name',identity,'--label','com.docker.compose.project='+meta['project'],'--label','sparkdq.purpose=t10-jdbc-fixture','--network',network,'--network-alias','jdbc-fixture','--memory','768m','--cpus','1','--env-file',str(env),'-v',volume+':/var/lib/mysql','mysql:8.4.8@sha256:2952e3be7807f06fc18de50b3ea1a632d5c70d63482ff7d7376fe3aa8999babf'],text=True,timeout=30).strip()
 deadline=time.monotonic()+80
 while True:
  probe=subprocess.run(['docker','exec',cid,'sh','-c','export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; mysql -u root -e "SELECT 1"'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=5)
  if probe.returncode==0:break
  assert time.monotonic()<deadline;time.sleep(1)
 with (out/'java-tests.log').open('x') as log:subprocess.run(['docker','run','--rm','--network',network,'--memory','2g','--cpus','1','--env-file',str(env),'-v',str(P)+':/src','-v','spark-dq-maven-cache:/root/.m2','-w','/src','maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d','mvn','-o','-B','-pl','backend','-Dtest=MysqlSnapshotIntegrationTest','test'],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
 report=P/'backend/target/surefire-reports/TEST-cn.sparkdq.MysqlSnapshotIntegrationTest.xml';node=ET.parse(report).getroot();counts={k:int(node.get(k,0)) for k in ['tests','errors','failures','skipped']};assert counts==dict(tests=10,errors=0,failures=0,skipped=0),counts;(out/report.name).write_bytes(report.read_bytes());result.update(ok=True,counts=counts,scope='Dedicated new MySQL fixture only; no metadata database or real business source',containerId=cid,network=network)
except Exception:result['failure']=traceback.format_exc();raise
finally:
 if cid:subprocess.run(['docker','stop','--timeout','5',cid],stdout=subprocess.DEVNULL,timeout=25,check=True)
 result['ownedFixtureStopped']=True;(out/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
