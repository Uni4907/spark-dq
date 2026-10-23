from pathlib import Path
import subprocess,time,json,hashlib,sys,shutil,xml.etree.ElementTree as ET
p=Path(__file__).resolve().parents[1];sys.path.insert(0,str(p/'infrastructure'));from algorithm_experiments import MODULES
root=p/'.runtime'/('build-candidate-'+time.strftime('%Y%m%d-%H%M%S'));root.mkdir(mode=0o700);image='maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d';opens=' '.join('--add-opens=java.base/'+m+'=ALL-UNNAMED' for m in MODULES)
with (root/'java-tests.log').open('x') as log:subprocess.run(['docker','run','--rm','--network','none','--hostname','t11-maven','--add-host','t11-maven:127.0.0.1','--cpus','2','--memory','4g','-e','SPARK_LOCAL_IP=127.0.0.1','-e','JAVA_TOOL_OPTIONS='+opens,'-v',str(p)+':/src','-v','spark-dq-maven-cache:/root/.m2','-w','/src',image,'mvn','-o','-B','package'],stdout=log,stderr=subprocess.STDOUT,timeout=480,check=True)
counts={}
for module in ['backend','spark-engine']:
 reports=p/module/'target/surefire-reports';shutil.copytree(reports,root/(module+'-reports'));counts[module]={key:sum(int(ET.parse(f).getroot().get(key,0)) for f in reports.glob('TEST-*.xml')) for key in ['tests','failures','errors','skipped']}
tags={}
for name in ['backend','frontend']:
 tag='spark-dq-'+name+':candidate-'+time.strftime('%H%M%S');tags[name+'Image']=tag
 with (root/(name+'-build.log')).open('x') as log:subprocess.run(['docker','build','--pull=false','-t',tag,'-f','infrastructure/'+name+'.Dockerfile','.'],cwd=p,stdout=log,stderr=subprocess.STDOUT,timeout=300,check=True)
 tags[name+'ImageId']=subprocess.check_output(['docker','image','inspect','--format','{{.Id}}',tag],text=True).strip()
with (root/'node-tests.log').open('x') as log:subprocess.run(['docker','run','--rm','--network','none','--entrypoint','node',tags['frontendImage'],'--test'],stdout=log,stderr=subprocess.STDOUT,timeout=60,check=True)
with (root/'python-tests.log').open('x') as log:subprocess.run(['python3','-m','unittest','discover','-s','tests','-p','test_*.py'],cwd=p,stdout=log,stderr=subprocess.STDOUT,timeout=90,check=True)
tags['engineJarSha256']=hashlib.sha256((p/'spark-engine/target/spark-engine-0.1.0.jar').read_bytes()).hexdigest();tags['java']=counts;(root/'candidate.json').write_text(json.dumps(tags,indent=2));print(json.dumps(dict(root=str(root),**tags)),flush=True)
