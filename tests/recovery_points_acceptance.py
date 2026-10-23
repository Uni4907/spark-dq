"""Only the new restore clone receives synthetic writes or failure injection."""
from pathlib import Path
import sys,json,uuid,shutil,os,hashlib,subprocess,argparse
r=Path(__file__).resolve().parents[1];sys.path.insert(0,str(r/'infrastructure'));sys.path.insert(0,str(r/'tests'))
import storage as st,maintenance as m,backup_catalog as bc,benchmark as b,e2e,native_imports_e2e as n
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--backup',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--fixture',type=Path,required=True);p.add_argument('--watch-primary',action='store_true');p.add_argument('--resume',action='store_true',help='Reuse verified legacy restore and first backup only; later partial stages require a fresh output directory');a=p.parse_args()
root=a.output.absolute();old=a.backup.absolute();source_manifest=st.verify(old);fixture=json.loads(a.fixture.read_text())
if root.exists() and not a.resume:raise st.StorageError('Refusing to reuse output without --resume')
root.mkdir(mode=0o700,exist_ok=a.resume)
if a.resume and (root/'second-backup').exists():raise st.StorageError('Later-stage evidence exists; preserve it and use a fresh output directory')
if a.watch_primary and not (root/'main-before.json').exists():
 st.save_json(root/'main-before.json',b.source_snapshot());st.save_json(root/'main-point-before.json',st.recovery_point(st.Project()))
if a.resume and (root/'legacy-restore/verified.json').exists():
 receipt=json.loads((root/'legacy-restore/verified.json').read_text());assert receipt['sourceManifestSha256']==st.digest(old/'manifest.json')['sha256']
else:
 with st.lock():receipt=st.restore(old,root/'legacy-restore')
clone=st.Project(Path(receipt['config']),root/'legacy-restore/.env');control=m.Control(clone);control.wait();e2e.BASE=control.address();n.check(fixture)
assert 'spark-dq-restore-' in Path(receipt['config']).read_text()
if 'recoveryPoint' in source_manifest:assert st.recovery_point(clone)['sha256']==source_manifest['recoveryPoint']['sha256']
report=bc.catalog(old,verify_latest=1);assert report['entries'][0]['metadataValid'] and report['verifiedCount']==1
source_coverage=bc.business_coverage(report,clone)['status'];assert source_coverage==('unknown' if 'recoveryPoint' not in source_manifest else 'matches')
print('PASS old V5 backup catalog and unchanged legacy restore; absent recovery point stays unknown',flush=True)
if not (a.resume and (root/'first-backup/backup/COMPLETE').exists()):m.backup_once(clone,root/'first-backup')
e2e.BASE=control.address();first_folder=root/'first-backup/backup';first_manifest=st.verify(first_folder);point=first_manifest['recoveryPoint'];assert st.validate_recovery_point(point,first_manifest['counts'])
report=bc.catalog(first_folder,verify_latest=1);match=bc.business_coverage(report,clone);assert match['status']=='matches'
if not (root/'matching.json').exists():st.save_json(root/'matching.json',match)
else:assert a.resume and json.loads((root/'matching.json').read_text())['currentFingerprint']==match['currentFingerprint']
print('PASS new backup records a bound, matching business recovery point',flush=True)
code,data=e2e.upload(b'id,name\n1,recovery-point-synthetic\n','recovery-point-synthetic.csv');assert code==200,data
changed=bc.business_coverage(bc.catalog(first_folder,verify_latest=1),clone);assert changed['status']=='changed' and any(row['table']=='datasets' and row['currentRows']==row['backupRows']+1 for row in changed['changedTables']);st.save_json(root/'added-record.json',changed)
m.backup_once(clone,root/'second-backup');second_folder=root/'second-backup/backup';second_manifest=st.verify(second_folder)
before=st.recovery_point(clone)
identity=data['id'];assert len(identity)==36 and set(identity)<=set('0123456789abcdef-')
# This row was created by this script in the isolated clone, never in the primary.
clone.mysql("UPDATE datasets SET name='recovery-point-synthetic-renamed.csv' WHERE id='"+identity+"';")
after=st.recovery_point(clone);assert before['sha256']!=after['sha256']
same_count=bc.business_coverage(bc.catalog(second_folder,verify_latest=1),clone);assert same_count['status']=='changed' and same_count['changedTables']==[{'table':'datasets','backupRows':second_manifest['counts']['datasets'],'currentRows':second_manifest['counts']['datasets'],'reason':'record-or-schema-fingerprint'}],same_count
st.save_json(root/'same-count-change.json',same_count)
print('PASS new record and same-count content change are both detected; primary untouched',flush=True)
# Exercise the actual CLI, including its nonzero exit for uncovered records.
for label,folder,expected in [('changed-cli',second_folder,2)]:
 output=root/(label+'.json');args=[sys.executable,'infrastructure/backup_catalog.py','--root',str(folder),'--verify-latest','1','--compare-current','--config',str(receipt['config']),'--env',str(root/'legacy-restore/.env'),'--output',str(output)]
 run=subprocess.run(args,cwd=r,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=180);assert run.returncode==expected,(run.returncode,run.stderr.decode());assert json.loads(output.read_text())['businessCoverage']['status']=='changed'
with st.lock():restored=st.restore(second_folder,root/'point-restore')
restored_project=st.Project(Path(restored['config']),root/'point-restore/.env');restored_control=m.Control(restored_project);restored_control.wait();e2e.BASE=restored_control.address();n.check(fixture)
actual=json.loads((root/'point-restore/recovery-point.json').read_text());assert actual['sha256']==second_manifest['recoveryPoint']['sha256']
assert e2e.http('/datasets/'+identity)[1]['name']=='recovery-point-synthetic.csv'
print('PASS fresh-volume restore verifies exact business fingerprint, native source, tasks and report',flush=True)
# A self-consistent but semantically wrong point must fail before backend startup.
fault=root/'controlled-wrong-point';fault.mkdir(mode=0o700)
for name in bc.ARTIFACTS:os.link(second_folder/name,fault/name)
wrong=json.loads((second_folder/'manifest.json').read_text());wrong['recoveryPoint']=after
st.save_json(fault/'manifest.json',wrong);st.save_json(fault/'COMPLETE',st.digest(fault/'manifest.json'));st.verify(fault)
try:
 with st.lock():st.restore(fault,root/'rejected-restore')
 raise AssertionError('Wrong recovery point accepted')
except st.StorageError as error:assert 'business records differ' in str(error),str(error)
(fault/'COMPLETE').rename(fault/'EXPECTED_RESTORE_REJECTION')
print('PASS mismatched logical point is refused before application startup; fault evidence retained',flush=True)
if a.watch_primary:
 assert json.loads((root/'main-before.json').read_text())==b.source_snapshot()
 assert st.recovery_point(st.Project())['sha256']==json.loads((root/'main-point-before.json').read_text())['sha256']
result={'ok':True,'legacyV5Restore':True,'legacyCoverage':source_coverage,'newBackupMatches':True,'addedRecordDetected':True,'sameCountChangeDetected':True,'changedCliExit':2,'newVolumeFingerprintVerified':True,'wrongPointRestoreRefused':True,'primaryUnchanged':True if a.watch_primary else None,'syntheticDatasetId':identity,'backup':str(second_folder),'restoredBusinessSha256':actual['sha256']}
st.save_json(root/'acceptance-verified.json',result)
st.run(restored_project.compose+['stop'],timeout=90);st.run(clone.compose+['stop'],timeout=90)
print(json.dumps(result),flush=True)
