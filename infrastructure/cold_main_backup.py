from pathlib import Path
import json,time,subprocess,sys,hashlib,shutil,os
P=Path('/home/sparkdq/projects/spark-dq');sys.path.insert(0,str(P/'infrastructure'));import algorithm_experiments as ex
root=P/'.runtime'/('t10-main-cold-'+time.strftime('%Y%m%d-%H%M%S'));root.mkdir(mode=0o700)
def output(args):return subprocess.check_output(args,text=True,timeout=60).strip()
def state():
 ids=output(['docker','ps','-aq','--filter','label=com.docker.compose.project=spark-dq']).split();nodes=json.loads(output(['docker','inspect',*ids]));assert len(nodes)==3 and not any(n['State']['Running'] for n in nodes)
 return [dict(id=n['Id'],name=n['Name'],imageId=n['Image'],running=n['State']['Running'],mounts=[dict(type=m['Type'],name=m.get('Name'),destination=m['Destination']) for m in n['Mounts']]) for n in nodes]
before=state();assert min(shutil.disk_usage(root).free,shutil.disk_usage('/mnt/c').free)>8*1024**3
fingerprint=ex.primary_fingerprints();(root/'source-before.json').write_text(json.dumps(fingerprint,indent=2))
image='maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d'
for volume,name in [('spark-dq_mysql-data','mysql-volume.tar.gz'),('spark-dq_files','files-volume.tar.gz')]:
 with (root/(name+'.log')).open('x') as log:subprocess.run(['docker','run','--rm','--network','none','--memory','512m','--cpus','1','--mount','type=volume,src='+volume+',dst=/from,readonly','--mount','type=bind,src='+str(root)+',dst=/out','--entrypoint','tar',image,'--numeric-owner','-czpf','/out/'+name,'-C','/from','.'],check=True,stdout=log,stderr=subprocess.STDOUT,timeout=180)
after=state();assert after==before and ex.primary_fingerprints()==fingerprint
shutil.copyfile(P/'.env',root/'private.env');(root/'private.env').chmod(0o600)
files={f.name:dict(bytes=f.stat().st_size,sha256=hashlib.sha256(f.read_bytes()).hexdigest()) for f in root.glob('*volume.tar.gz')}
receipt=dict(ok=True,scope='Read-only cold physical backup of stopped actual main deployment; no main containers started or modified',containers=before,sourceFingerprints=fingerprint,files=files,mysqlVersion='8.4.8 pinned original image',sourceReadOnly=True,mainBeforeAfterIdentical=True,privateCredentials='private.env retained locally, never in public evidence',createdUtc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
(root/'receipt.json').write_text(json.dumps(receipt,indent=2));print(root,flush=True)
