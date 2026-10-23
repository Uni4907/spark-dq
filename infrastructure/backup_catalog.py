#!/usr/bin/env python3
"""Read-only backup inventory/capacity/age checks; never deletes or restores."""
import argparse,datetime as dt,json,os,re,shutil,sys
from pathlib import Path
import storage as st

UTC=dt.timezone.utc
ARTIFACTS={'database.sql','files.tar','application.tar','audit.json'}

def small_json(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size>2*1024*1024:
        raise st.StorageError('Missing/unsafe/oversized metadata')
    return json.loads(path.read_text())

def inspect(folder):
    entry={'path':str(folder),'metadataValid':False,'verification':'not-run'}
    try:
        manifest=small_json(folder/'manifest.json');complete=small_json(folder/'COMPLETE')
        if complete!=st.digest(folder/'manifest.json'):raise st.StorageError('Manifest checksum mismatch')
        if manifest.get('formatVersion')!='sparkdq-backup-v1' or set(manifest.get('artifacts',{}))!=ARTIFACTS:
            raise st.StorageError('Unsupported backup layout')
        created=dt.datetime.fromisoformat(manifest['createdAt'])
        if created.tzinfo is None:raise st.StorageError('Backup timestamp must include timezone')
        total=0
        for name,record in manifest['artifacts'].items():
            file=folder/name
            if not re.fullmatch('[0-9a-f]{64}',record['sha256']) or type(record['size']) is not int or record['size']<0:
                raise st.StorageError('Invalid artifact metadata')
            if file.is_symlink() or not file.is_file() or file.stat().st_size!=record['size']:
                raise st.StorageError('Artifact size/membership mismatch')
            total+=record['size']
        counts=manifest['counts']
        if not st.supported_tables(counts):
            raise st.StorageError('Unsupported table layout')
        if any(type(n) is not int or n<0 for n in counts.values()):raise st.StorageError('Invalid table counts')
        if 'recoveryPoint' in manifest:
            st.validate_recovery_point(manifest['recoveryPoint'],counts)
            entry['businessFingerprint']=manifest['recoveryPoint']['sha256']
            entry['recoveryPoint']='recorded'
        else:entry['recoveryPoint']='not-recorded'
        entry.update(metadataValid=True,createdAt=created.astimezone(UTC).isoformat(),artifactBytes=total,counts=counts)
    except (OSError,ValueError,KeyError,TypeError,AttributeError,st.StorageError) as error:
        entry['issue']=str(error) if isinstance(error,st.StorageError) else type(error).__name__
    return entry

def catalog(root,keep=3,verify_latest=0,max_age_hours=24,reserve_mib=1024):
    root=root.resolve()
    if not root.is_dir():raise st.StorageError('Backup search root must exist')
    entries=[];scan_errors=[]
    def scan_error(error):
        scan_errors.append({'path':str(error.filename or root),'type':type(error).__name__})
    for current,dirs,files in os.walk(root,followlinks=False,onerror=scan_error):
        dirs[:]=sorted(d for d in dirs if not (Path(current)/d).is_symlink())
        if any(name in files for name in ('manifest.json','COMPLETE','database.sql')):
            entries.append(inspect(Path(current)))
            # A backup is a leaf; never inspect archive/data content as backup directories.
            dirs[:]=[]
    entries.sort(key=lambda e:(e.get('createdAt',''),e['path']),reverse=True)
    healthy=[e for e in entries if e['metadataValid']]
    verified=[]
    for entry in healthy:
        if len(verified)>=verify_latest:break
        try:
            st.verify(Path(entry['path']));entry['verification']='passed';verified.append(entry)
        except (OSError,st.StorageError) as error:
            entry.update(verification='failed',issue=type(error).__name__)
    good=[e for e in healthy if e['verification']!='failed']
    for entry in entries:
        entry['retentionAdvice']='keep' if entry in good[:keep] else 'review-only' if entry in good else 'inspect'
    now=dt.datetime.now(UTC)
    latest=verified[0] if verified else None
    age=max(0,(now-dt.datetime.fromisoformat(latest['createdAt'])).total_seconds()/3600) if latest else None
    space=shutil.disk_usage(root)
    estimate=max((e['artifactBytes'] for e in good),default=0)
    required=int(estimate*1.25)+reserve_mib*1024**2
    return {'formatVersion':'sparkdq-backup-catalog-v1','createdAt':now.isoformat(),'root':str(root),
      'entries':entries,'scanComplete':not scan_errors,'scanErrors':scan_errors,'metadataValidCount':len(healthy),'verifiedCount':len(verified),
      'latestVerified':latest['path'] if latest else None,'latestVerifiedAgeHours':round(age,3) if age is not None else None,
      'freshVerifiedBackup':age is not None and age<=max_age_hours,'maxAgeHours':max_age_hours,
      'disk':{'freeBytes':space.free,'totalBytes':space.total,'reserveBytes':reserve_mib*1024**2,
        'estimatedNextBackupBytes':estimate,'requiredFreeBytesEstimate':required,
        'enoughForEstimate':space.free>=required},
      'actionsPerformed':[],'note':'Metadata/size checks are not full verification. Retention advice is review-only; no deletion, backup, restore or service changes.'}

def business_coverage(report,project):
    result={'status':'unknown','matchingBackup':None,'currentFingerprint':None,'changedTables':[],
      'scope':'Business database records at one read-only snapshot; not live-file integrity, grants, application identity or automatic restore readiness.'}
    try:current=st.recovery_point(project)
    except (OSError,ValueError,st.StorageError) as error:
        result.update(status='unavailable',issue=type(error).__name__);return result
    result.update(currentFingerprint=current['sha256'],capturedAt=current['capturedAt'])
    candidates=[]
    for entry in report['entries']:
        if entry['verification']!='passed':continue
        # Read the bound metadata again: changed manifests cannot reuse an earlier verification.
        try:
            if small_json(Path(entry['path'])/'COMPLETE')!=st.digest(Path(entry['path'])/'manifest.json'):raise st.StorageError('Manifest changed during comparison')
            manifest=small_json(Path(entry['path'])/'manifest.json')
            point=manifest.get('recoveryPoint')
            if point is None:entry['businessCoverage']='not-recorded';continue
            st.validate_recovery_point(point,manifest['counts'])
            if point['sha256']!=entry.get('businessFingerprint'):raise st.StorageError('Recovery point changed during comparison')
        except (OSError,ValueError,KeyError,TypeError,AttributeError,st.StorageError):
            entry['businessCoverage']='unavailable';continue
        same=point['sha256']==current['sha256'];entry['businessCoverage']='matches' if same else 'changed'
        candidates.append((entry,point))
        if same and result['matchingBackup'] is None:result.update(status='matches',matchingBackup=entry['path'])
    if not candidates:return result
    if result['status']=='matches':return result
    result['status']='changed';entry,point=candidates[0]
    result['comparedBackup']=entry['path']
    for table in sorted(set(point['tables'])|set(current['tables'])):
        before=point['tables'].get(table);after=current['tables'].get(table)
        if before!=after:result['changedTables'].append({'table':table,'backupRows':before['rows'] if before else None,'currentRows':after['rows'] if after else None,
            'reason':'table-layout' if before is None or after is None else 'record-or-schema-fingerprint'})
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,default=st.ROOT/'.runtime')
    p.add_argument('--keep',type=int,default=3);p.add_argument('--verify-latest',type=int,default=0)
    p.add_argument('--max-age-hours',type=int,default=24);p.add_argument('--reserve-mib',type=int,default=1024)
    p.add_argument('--output',type=Path)
    p.add_argument('--compare-current',action='store_true',help='Compare verified recovery points with a read-only transaction on the local project')
    p.add_argument('--config',type=Path);p.add_argument('--env',type=Path)
    args=p.parse_args()
    if not 1<=args.keep<=100 or not 0<=args.verify_latest<=20 or not 1<=args.max_age_hours<=8760 or not 256<=args.reserve_mib<=1048576:
        p.error('Limits: keep 1..100, verify-latest 0..20, age 1..8760 hours, reserve 256..1048576 MiB')
    if args.compare_current and not args.verify_latest:p.error('--compare-current requires --verify-latest >= 1')
    if (args.config or args.env) and not args.compare_current:p.error('--config/--env require --compare-current')
    try:
        if args.output and args.output.exists():raise st.StorageError('Refusing to overwrite output')
        result=catalog(args.root,args.keep,args.verify_latest,args.max_age_hours,args.reserve_mib)
        if args.compare_current:result['businessCoverage']=business_coverage(result,st.Project(args.config,args.env))
        if args.output:st.save_json(args.output,result)
        print(json.dumps(result,ensure_ascii=False))
        return 0 if result['scanComplete'] and result['disk']['enoughForEstimate'] and (not args.verify_latest or result['freshVerifiedBackup']) and (not args.compare_current or result['businessCoverage']['status']=='matches') else 2
    except (OSError,st.StorageError) as error:
        print(json.dumps({'error':str(error),'type':type(error).__name__}),file=sys.stderr);return 1

if __name__=='__main__':sys.exit(main())
