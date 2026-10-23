#!/usr/bin/env python3
"""Offline read-only audit, consistent backup, fresh isolated restore; stdlib only.

Never restores into existing volumes or deletes data. Requires project Docker
permission. Audit/backup refuse a running backend or unfinished task states.
"""
import argparse, contextlib, datetime as dt, fcntl, hashlib, json, os, re, secrets
from pathlib import Path, PurePosixPath
import subprocess, sys, tarfile, tempfile, threading, time, urllib.request, uuid

ROOT=Path(__file__).resolve().parent.parent
MYSQL_IMAGE='mysql:8.4.8@sha256:2952e3be7807f06fc18de50b3ea1a632d5c70d63482ff7d7376fe3aa8999babf'
BACKEND_IMAGE='spark-dq-backend:0.1.0'
TABLES=('datasets','tasks','rules','results','task_snapshots','governance_runs',
 'dataset_versions','rule_parameters','rule_template_heads',
 'rule_template_versions','task_template_refs','governance_audits')
def supported_tables(names):
    return set(names) in (set(TABLES),set(TABLES)|{'flyway_schema_history'},set(TABLES)|{'flyway_schema_history','dataset_imports'})

MAX_POINT_ROWS=100000
POINT_ALGORITHM='SHA256_SORTED_MYSQL_JSON_ROWS_V1'
MAX_BYTES=4*1024**3
MAX_MEMBERS=1000000
LOCAL=urllib.request.build_opener(urllib.request.ProxyHandler({}))

class StorageError(RuntimeError):pass

def run(args,*,data=None,output=None,timeout=120):
    try:
        process=subprocess.Popen(args,stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
          stdout=output or subprocess.PIPE,stderr=subprocess.PIPE,cwd=ROOT)
    except OSError as error:
        raise StorageError('Local operation failed or timed out: '+type(error).__name__) from None
    try:
        stdout,stderr=process.communicate(input=data,timeout=timeout)
    except BaseException as error:
        # Own only this child. Do not let an interrupt make Popen.__exit__ wait forever.
        with contextlib.suppress(OSError):process.kill()
        with contextlib.suppress(OSError,subprocess.TimeoutExpired):process.communicate(timeout=5)
        if isinstance(error,(OSError,subprocess.TimeoutExpired)):
            raise StorageError('Local operation failed or timed out: '+type(error).__name__) from None
        raise
    if process.returncode:
        # No credentials, SQL or arbitrary subprocess output in diagnostics.
        raise StorageError('Local '+str(args[0])+' '+str(args[1])+' operation failed (exit '+str(process.returncode)+')')
    return stdout if output is None else None

def save_json(path,value):
    with path.open('x',encoding='utf-8') as stream:
        os.chmod(path,0o600);json.dump(value,stream,ensure_ascii=False,indent=2);stream.write('\n')

def digest(path):
    sha=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):sha.update(chunk)
    return {'sha256':sha.hexdigest(),'size':path.stat().st_size}

def safe_name(name):
    p=PurePosixPath(name)
    if p.is_absolute() or '..' in p.parts:raise StorageError('Unsafe archive path')
    return '' if str(p)=='.' else str(p)

def scan_tar(stream):
    """Read only; reject traversal, links, special entries, duplicates and bombs."""
    files,directories,seen={},set(),set();size=0;prepared={};captured=0
    with tarfile.open(fileobj=stream,mode='r|') as archive:
        for index,member in enumerate(archive):
            if index>=MAX_MEMBERS:raise StorageError('Too many archive entries')
            name=safe_name(member.name)
            if not name and member.isdir():continue
            if not name or name in seen:raise StorageError('Duplicate/empty archive entry')
            seen.add(name)
            if member.isdir():directories.add(name);continue
            if not member.isfile():raise StorageError('Links/special archive entries unsupported')
            size+=member.size
            if member.size<0 or size>MAX_BYTES:raise StorageError('Archive exceeds 4 GiB MVP limit')
            sha=hashlib.sha256();entry=archive.extractfile(member);actual=0
            capture=bool(re.fullmatch(r'(?:(?:tasks|imports)/[^/]+/prepared|mysql-imports/[^/]+/snapshot)/manifest\.json',name))
            if capture:
                captured+=min(member.size,1048577)
                if captured>16*1024**2:raise StorageError('Prepared manifest metadata exceeds 16 MiB audit budget')
            content=bytearray() if capture and member.size<=1048576 else None
            for chunk in iter(lambda:entry.read(1024*1024),b''):
                sha.update(chunk);actual+=len(chunk)
                if content is not None:content.extend(chunk)
            if actual!=member.size:raise StorageError('Truncated archive entry')
            files[name]={'size':member.size,'sha256':sha.hexdigest()}
            if capture:
                try:prepared[name]=json.loads(content) if content is not None else {'_invalid':True}
                except (ValueError,UnicodeError):prepared[name]={'_invalid':True}
    result={'files':files,'directories':sorted(directories),'bytes':size}
    if prepared:result['preparedManifests']=prepared
    return result

class Project:
    def __init__(self,config=None,env=None):
        self.compose=['docker','compose','--env-file',str(Path(env or ROOT/'.env').absolute()),
          '-f',str(Path(config or ROOT/'infrastructure/compose.yaml').absolute())]
    def service(self,name):
        ids=run(self.compose+['ps','--all','-q',name]).decode().split()
        if len(ids)!=1:raise StorageError('Expected one '+name+' container')
        return ids[0]
    def running(self,name):
        return run(['docker','inspect','--format','{{.State.Running}}',self.service(name)]).strip()==b'true'
    def volume(self,name,destination):
        mounts=json.loads(run(['docker','inspect','--format','{{json .Mounts}}',self.service(name)]))
        choices=[m['Name'] for m in mounts if m['Type']=='volume' and m['Destination']==destination]
        if len(choices)!=1:raise StorageError('Expected a named data volume')
        return choices[0]
    def offline(self):
        if self.running('backend'):raise StorageError('Stop the idle backend in a maintenance window first')
        if not self.running('mysql'):raise StorageError('MySQL must be running')
    def mysql(self,sql=None,output=None,dump=False):
        command=('exec mysqldump --default-character-set=utf8mb4 --user=root --single-transaction --no-tablespaces '
          '--skip-comments --set-gtid-purged=OFF --databases "$MYSQL_DATABASE"') if dump else (
          'exec mysql --binary-mode --default-character-set=utf8mb4 --user=root --database="$MYSQL_DATABASE" --batch --raw --skip-column-names')
        return run(['docker','exec','-i',self.service('mysql'),'sh','-c',
          'export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; '+command],
          data=sql.encode() if isinstance(sql,str) else sql,output=output,timeout=180)
    def rows(self,sql):
        return [json.loads(line) for line in self.mysql(sql).decode().splitlines() if line]
    def inventory(self):
        tables=self.mysql('SHOW TABLES;').decode().splitlines()
        if not supported_tables(tables):
            raise StorageError('Unsupported database table set')
        counts={table:int(self.mysql('SELECT COUNT(*) FROM '+table+';').strip()) for table in sorted(tables)}
        active=self.rows("SELECT JSON_OBJECT('id',id,'status',status) FROM tasks WHERE status IN ('QUEUED','RUNNING','CANCELLING');")
        if active:raise StorageError('Unfinished task states; restart/recover before backup')
        columns=self.rows("SELECT JSON_OBJECT('n',COUNT(*)) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='task_snapshots' AND COLUMN_NAME='execution_plan_json';")
        prepared=self.rows("SELECT JSON_OBJECT('taskId',t.id,'status',t.status,'sourceSha256',s.input_sha256,'plan',s.execution_plan_json,'manifestSha256',s.input_manifest_sha256) FROM task_snapshots s JOIN tasks t ON t.id=s.task_id WHERE s.execution_plan_json IS NOT NULL ORDER BY t.id;") if columns and columns[0]['n']==1 else []
        source_columns=self.rows("SELECT JSON_OBJECT('n',COUNT(*)) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='datasets' AND COLUMN_NAME='source_manifest_json';")
        imports=self.rows("SELECT JSON_OBJECT('id',id,'sha256',sha256,'rowCount',row_count,'columns',columns_json,'manifest',source_manifest_json) FROM datasets WHERE source_manifest_json IS NOT NULL ORDER BY id;") if source_columns and source_columns[0]['n']==1 else []
        kinds=self.rows("SELECT JSON_OBJECT('n',COUNT(*)) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='dataset_imports' AND COLUMN_NAME='source_kind';") if 'dataset_imports' in tables else []
        extra=",'sourceKind',source_kind,'request',source_request_json" if kinds and kinds[0]['n']==1 else ''
        native=self.rows("SELECT JSON_OBJECT('id',id,'status',status,'name',name,'sourceSha256',source_sha256,'sourceBytes',source_bytes,'contractVersion',contract_version,'datasetId',dataset_id,'manifestSha256',manifest_sha256"+extra+") FROM dataset_imports ORDER BY id;") if 'dataset_imports' in tables else []
        if any(row['status'] in ('QUEUED','RUNNING','CANCELLING') for row in native):raise StorageError('Unfinished import states; restart/recover before backup')
        if any(row.get('sourceKind','PARQUET_NATIVE') not in ('PARQUET_NATIVE','MYSQL_SNAPSHOT') for row in native):raise StorageError('Unsupported import source kind')
        business=[row for row in native if row.get('sourceKind')=='MYSQL_SNAPSHOT']
        native=[row for row in native if row.get('sourceKind','PARQUET_NATIVE')=='PARQUET_NATIVE']
        return {'mysqlImports':business,'nativeImports':native,'sourceImports':imports,'preparedInputs':prepared,'counts':counts,
         'datasets':self.rows("SELECT JSON_OBJECT('id',id,'path',file_path,'sha256',sha256,'size',size_bytes) FROM datasets ORDER BY id;"),
         'tasks':self.rows("SELECT JSON_OBJECT('id',id,'status',status) FROM tasks ORDER BY id;"),
         'results':self.rows("SELECT JSON_OBJECT('ruleId',r.id,'taskId',r.task_id,'status',t.status,'path',s.anomaly_path) FROM results s JOIN rules r ON s.rule_id=r.id JOIN tasks t ON t.id=r.task_id ORDER BY r.id;"),
         'snapshotMismatches':self.rows("SELECT JSON_OBJECT('taskId',s.task_id) FROM task_snapshots s JOIN tasks t ON t.id=s.task_id JOIN datasets d ON d.id=t.dataset_id WHERE s.input_sha256<>d.sha256;"),
         'missingResults':self.rows("SELECT JSON_OBJECT('ruleId',r.id,'taskId',t.id) FROM tasks t JOIN rules r ON r.task_id=t.id LEFT JOIN results s ON s.rule_id=r.id WHERE t.status='SUCCEEDED' AND s.id IS NULL;")}

def point_digest(tables):
    body=json.dumps({'algorithm':POINT_ALGORITHM,'tables':tables},sort_keys=True,separators=(',',':'),ensure_ascii=True)
    return hashlib.sha256(body.encode('ascii')).hexdigest()

def validate_recovery_point(point,counts=None):
    try:
        if not isinstance(point,dict) or set(point)!={'formatVersion','algorithm','capturedAt','tables','sha256'}:raise ValueError()
        if point['formatVersion']!='sparkdq-recovery-point-v1' or point['algorithm']!=POINT_ALGORITHM:raise ValueError()
        captured=dt.datetime.fromisoformat(point['capturedAt'])
        if captured.tzinfo is None:raise ValueError()
        tables=point['tables']
        if not isinstance(tables,dict) or set(tables) not in (set(TABLES),set(TABLES)|{'dataset_imports'}):raise ValueError()
        if counts is not None and (not supported_tables(counts) or set(tables)!=set(counts)-{'flyway_schema_history'}):raise ValueError()
        total=0
        for name,record in tables.items():
            if not isinstance(record,dict) or set(record)!={'rows','sha256','schemaSha256'} or type(record['rows']) is not int or record['rows']<0:raise ValueError()
            if any(not isinstance(record[k],str) or not re.fullmatch('[0-9a-f]{64}',record[k]) for k in ('sha256','schemaSha256')):raise ValueError()
            total+=record['rows']
            if counts is not None and (type(counts[name]) is not int or counts[name]!=record['rows']):raise ValueError()
        if total>MAX_POINT_ROWS or point['sha256']!=point_digest(tables):raise ValueError()
    except (ValueError,KeyError,TypeError,AttributeError):raise StorageError('Invalid business recovery point') from None
    return point

def recovery_point(project):
    """One read-only repeatable-read transaction; Business row values never leave MySQL, only their hashes.

    The application business tables are hashed, excluding volatile Flyway history. This
    compares business RECORDS, not live files, grants, application images or restore readiness.
    """
    project_tables=project.mysql('SHOW TABLES;').decode().splitlines()
    names=project_tables
    if not supported_tables(names):raise StorageError('Unsupported database table set for recovery point')
    names=sorted(set(names)-{'flyway_schema_history'})
    schema_sql="SELECT JSON_OBJECT('table',TABLE_NAME,'name',COLUMN_NAME,'type',COLUMN_TYPE,'nullable',IS_NULLABLE,'collation',COALESCE(COLLATION_NAME,'')) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() ORDER BY TABLE_NAME,ORDINAL_POSITION;"
    metadata=project.rows(schema_sql)
    schemas={name:[] for name in names}
    for column in metadata:
        if column.get('table') in schemas:schemas[column['table']].append(column)
    if len(metadata)>2048 or any(not columns or len(columns)>100 for columns in schemas.values()):raise StorageError('Recovery point column budget exceeded')
    commands=["SET time_zone='+00:00';SET SESSION MAX_EXECUTION_TIME=5000;SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY;"]
    for name in names:
        columns=[column['name'] for column in schemas[name]]
        if any(not re.fullmatch('[A-Za-z][A-Za-z0-9_]*',column) for column in columns):raise StorageError('Unsupported recovery point column identifier')
        selected=','.join('`'+column+'`' for column in columns)
        commands.append("SELECT 'SPARKDQ_POINT_"+name+"';SELECT SHA2(CAST(JSON_ARRAY("+selected+") AS CHAR CHARACTER SET utf8mb4),256) FROM `"+name+"` ORDER BY 1 LIMIT "+str(MAX_POINT_ROWS+1)+';')
    commands.append('COMMIT;')
    captured=dt.datetime.now(dt.timezone.utc).isoformat()
    rows={name:[] for name in names};current=None;total=0;seen=set()
    for line in project.mysql('\n'.join(commands)).decode('ascii').splitlines():
        if line.startswith('SPARKDQ_POINT_'):
            current=line[len('SPARKDQ_POINT_'):]
            if current not in rows or current in seen:raise StorageError('Invalid recovery point response')
            seen.add(current)
        elif current is None or not re.fullmatch('[0-9a-f]{64}',line):raise StorageError('Invalid recovery point row digest')
        else:
            total+=1
            if total>MAX_POINT_ROWS:raise StorageError('Business recovery point exceeds 100000 record budget')
            rows[current].append(line)
    if seen!=set(names):raise StorageError('Incomplete recovery point response')
    # DDL is outside InnoDB row snapshots. Refuse a concurrent schema/layout change
    # instead of binding rows selected with stale column metadata.
    if set(project.mysql('SHOW TABLES;').decode().splitlines())!=set(project_tables):raise StorageError('Database layout changed during recovery point capture')
    after=project.rows(schema_sql)
    if [col for col in after if col.get('table') in schemas]!=[col for col in metadata if col.get('table') in schemas]:raise StorageError('Database schema changed during recovery point capture')
    tables={name:{'rows':len(rows[name]),'sha256':hashlib.sha256((''.join(row+'\n' for row in rows[name])).encode('ascii')).hexdigest(),
        'schemaSha256':hashlib.sha256(json.dumps(schemas[name],sort_keys=True,separators=(',',':'),ensure_ascii=True).encode('ascii')).hexdigest()} for name in names}
    return validate_recovery_point({'formatVersion':'sparkdq-recovery-point-v1','algorithm':POINT_ALGORITHM,'capturedAt':captured,'tables':tables,'sha256':point_digest(tables)})


def tar_command(volume):
    return ['docker','run','--rm','--network','none','--read-only','--cap-drop','ALL',
     '--security-opt','no-new-privileges','--mount','type=volume,source='+volume+',target=/data,readonly',
     '--entrypoint','tar',BACKEND_IMAGE,'-cf','-','-C','/data','.']

def volume_inventory(volume):
    with tempfile.TemporaryFile() as errors:
        process=subprocess.Popen(tar_command(volume),stdout=subprocess.PIPE,stderr=errors)
        timer=threading.Timer(180,process.kill);timer.daemon=True;timer.start()
        try:
            result=scan_tar(process.stdout)
            if process.wait(timeout=10):raise StorageError('Read-only volume scan failed')
            return result
        finally:
            timer.cancel();process.stdout.close()
            if process.poll() is None:process.kill()
            process.wait(timeout=10)

def compare(metadata,inventory):
    files=inventory['files'];issues,warnings=[],[];references=set()
    for row in metadata['datasets']:
        expected='raw/'+row['id']+'.csv';references.add(expected);actual=files.get(expected)
        if row['path']!='/data/'+expected:issues.append({'kind':'invalidDatasetPath','datasetId':row['id']})
        if actual is None:issues.append({'kind':'missingDatasetFile','datasetId':row['id'],'path':expected})
        elif actual!={'sha256':row['sha256'],'size':row['size']}:
            issues.append({'kind':'changedDatasetFile','datasetId':row['id'],'path':expected,
             'expected':{'sha256':row['sha256'],'size':row['size']},'actual':actual})
    warnings.extend({'kind':'unregisteredRawFile','path':p} for p in sorted(files)
      if p.startswith('raw/') and p not in references)
    known={t['id'] for t in metadata['tasks']}
    unknown={p.split('/')[1] for p in files if p.startswith('tasks/') and len(p.split('/'))>2}-known
    warnings.extend({'kind':'unregisteredTaskDirectory','taskId':t} for t in sorted(unknown))
    parts={}
    for path in files:
        p=PurePosixPath(path)
        if p.name.startswith('part-') and p.suffix=='.json':parts.setdefault(str(p.parent),[]).append(path)
    for row in metadata['results']:
        expected='tasks/'+row['taskId']+'/anomalies/'+row['ruleId']
        if row['status']!='SUCCEEDED':issues.append({'kind':'resultOnUnsuccessfulTask','taskId':row['taskId'],'ruleId':row['ruleId']})
        if row['path']!='/data/'+expected:issues.append({'kind':'invalidAnomalyPath','ruleId':row['ruleId']})
        if expected not in parts:issues.append({'kind':'missingAnomalyParts','taskId':row['taskId'],'ruleId':row['ruleId']})
    for row in metadata.get('preparedInputs',[]):
        prefix='tasks/'+row['taskId']+'/prepared/';name=prefix+'manifest.json';fingerprint=row.get('manifestSha256')
        if not fingerprint:
            if row['status']=='SUCCEEDED':issues.append({'kind':'successfulPreparedWithoutManifest','taskId':row['taskId']})
            continue # Failed/cancelled partial work is retained and never accepted as a completed artifact.
        if name not in files or files[name]['sha256']!=fingerprint:
            issues.append({'kind':'preparedManifestFingerprintMismatch','taskId':row['taskId']});continue
        node=inventory.get('preparedManifests',{}).get(name);plan=row['plan']
        if isinstance(plan,str):
            try:plan=json.loads(plan)
            except ValueError:plan=None
        try:
            if not isinstance(node,dict) or not isinstance(plan,dict):raise ValueError()
            if node.get('version')!=1 or node.get('semanticsVersion')!='csv-v2' or node.get('format')!=plan['format'] or node.get('sourceSha256')!=row['sourceSha256'] or node.get('rowCount')!=plan['rowCount'] or node.get('columns')!=plan['columns']:raise ValueError()
            parts=node['files']
            if not isinstance(parts,list) or not 1<=len(parts)<=256:raise ValueError()
            seen=set();total=0
            for entry in parts:
                path=entry['path'];suffix='.csv' if plan['format']=='CSV' else '.parquet'
                if not re.fullmatch(r'data/part-[A-Za-z0-9_.-]+',path) or not path.endswith(suffix) or path in seen:raise ValueError()
                seen.add(path);total+=entry['size']
                if total>256*1024**2 or files.get(prefix+path)!={'sha256':entry['sha256'],'size':entry['size']}:raise ValueError()
        except (ValueError,KeyError,TypeError):
            issues.append({'kind':'preparedArtifactMismatch','taskId':row['taskId']})
    imports=metadata.get('sourceImports',[]);registered=set()
    for row in imports:
        prefix='sources/'+row['id']+'/';registered.add(row['id'])
        try:
            node=row['manifest'];node=json.loads(node) if isinstance(node,str) else node
            columns=row['columns'];columns=json.loads(columns) if isinstance(columns,str) else columns
            if isinstance(node,dict) and node.get('format')=='CONTROLLED_CSV':
                if node.get('version')!=1 or node.get('profile')!='CONTROLLED_64M_V1' or node.get('kind') not in ('PRESET','GOVERNED') or node.get('canonicalSha256')!=row['sha256'] or node.get('columns')!=columns or node.get('rowCount')!=row['rowCount'] or not 0<=row['rowCount']<=1000000:raise ValueError()
                entries=node['files']
                if not isinstance(entries,list) or len(entries)!=1:raise ValueError()
                entry=entries[0];actual=files.get('raw/'+row['id']+'.csv')
                if entry.get('path')!='canonical.csv' or entry.get('sha256')!=row['sha256'] or entry.get('rows')!=row['rowCount'] or not isinstance(entry.get('size'),int) or not 0<entry['size']<=64*1024**2 or actual!={'sha256':entry['sha256'],'size':entry['size']}:raise ValueError()
                if 'hive' in node:
                    h=node['hive']
                    if node['kind']!='PRESET' or not isinstance(h,dict) or len(h)!=11 or h.get('version')!=1 or h.get('catalog')!='HIVE' or h.get('provider')!='hive' or h.get('metastore')!='EMBEDDED_DERBY' or h.get('sparkVersion')!='3.5.8' or h.get('canonicalSha256')!=row['sha256'] or h.get('columns')!=columns or h.get('rows')!=row['rowCount'] or not 1<=h['rows']<=100 or not re.fullmatch(r'[a-f0-9]{64}',h.get('receiptSha256','')) or any(not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}',h.get(key,'')) for key in ('database','table')):raise ValueError()
                if node['kind']=='PRESET' and not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,63}',node.get('sourceId','')):raise ValueError()
                if node['kind']=='GOVERNED' and (node.get('parentDatasetId')==row['id'] or not any(v['id']==node.get('parentDatasetId') for v in metadata['datasets'])):raise ValueError()
                continue
            if isinstance(node,dict) and node.get('format')=='PARQUET_NATIVE':
                validate_native_source(row,node,columns,metadata)
                continue
            if isinstance(node,dict) and node.get('format')=='MYSQL_SNAPSHOT':
                validate_mysql_source(row,node,columns,metadata,inventory)
                continue
            if not isinstance(node,dict) or node.get('version')!=1 or node.get('format')!='CSV_PARTS' or node.get('order')!='REQUEST' or node.get('canonicalSha256')!=row['sha256'] or node.get('rowCount')!=row['rowCount'] or node.get('columns')!=columns:raise ValueError()
            entries=node['files']
            if not isinstance(entries,list) or not 2<=len(entries)<=16:raise ValueError()
            total=0;rows=0;names=set()
            for index,entry in enumerate(entries,1):
                if entry['path']!=f'{index:03d}.csv' or entry['name'] in names or type(entry['size']) is not int or entry['size']<=0 or type(entry['rows']) is not int or entry['rows']<0:raise ValueError()
                names.add(entry['name']);total+=entry['size'];rows+=entry['rows']
                if total>20*1024**2 or files.get(prefix+entry['path'])!={'sha256':entry['sha256'],'size':entry['size']}:raise ValueError()
            if rows!=row['rowCount']:raise ValueError()
        except (ValueError,KeyError,TypeError):issues.append({'kind':'sourceImportMismatch','datasetId':row['id']})
    unknown_sources={p.split('/')[1] for p in files if p.startswith('sources/') and len(p.split('/'))>2}-registered
    warnings.extend({'kind':'unregisteredSourceDirectory','datasetId':identity} for identity in sorted(unknown_sources))
    compare_native(metadata,inventory,issues,warnings)
    compare_mysql(metadata,inventory,issues,warnings)
    issues.extend({'kind':'snapshotFingerprintMismatch',**r} for r in metadata['snapshotMismatches'])
    issues.extend({'kind':'successfulRuleWithoutResult',**r} for r in metadata['missingResults'])
    return {'formatVersion':'sparkdq-audit-v1','ok':not issues,'counts':metadata['counts'],
      'fileCount':len(files),'bytes':inventory['bytes'],'issues':issues,'warnings':warnings,
      'note':'Offline read-only audit. Known failed/cancelled task diagnostics are retained.'}


NATIVE_CONVERSION={'version':1,'timezone':'UTC','decimal':'PLAIN_WITH_SCALE','integer':'BASE10','boolean':'LOWERCASE','date':'GREGORIAN_1582_10_15_TO_9999','string':'STRICT_UTF8','order':'UNSPECIFIED'}

def validate_native_source(row,node,columns,metadata):
    imports=[job for job in metadata.get('nativeImports',[]) if job['id']==row['id']]
    if len(imports)!=1:raise ValueError()
    job=imports[0]
    if job['status']!='SUCCEEDED' or job['datasetId']!=row['id'] or node.get('version')!=1 or node.get('order')!='SINGLE' or node.get('importId')!=job['id'] or node.get('columns')!=columns or node.get('rowCount')!=row['rowCount'] or node.get('canonicalSha256')!=row['sha256'] or node.get('nativeManifestSha256')!=job['manifestSha256'] or node.get('conversion')!=NATIVE_CONVERSION:raise ValueError()
    expected={'path':'source.parquet','name':job['name'],'size':job['sourceBytes'],'sha256':job['sourceSha256'],'rows':row['rowCount']}
    if node.get('files')!=[expected] or not isinstance(node.get('nativeColumns'),list) or not all(isinstance(c,dict) for c in node['nativeColumns']) or [c.get('name') for c in node['nativeColumns']]!=columns:raise ValueError()

def compare_native(metadata,inventory,issues,warnings):
    files=inventory['files'];registered=set();sources={r['id']:r for r in metadata.get('sourceImports',[])}
    for job in metadata.get('nativeImports',[]):
        identity=job['id'];registered.add(identity);prefix='imports/'+identity+'/'
        try:
            if not re.fullmatch(r'[0-9a-f-]{36}',identity) or job['contractVersion']!=1 or type(job['sourceBytes']) is not int or not 12<=job['sourceBytes']<=20*1024**2 or not re.fullmatch('[0-9a-f]{64}',job['sourceSha256']):raise ValueError()
            if files.get(prefix+'source.parquet')!={'size':job['sourceBytes'],'sha256':job['sourceSha256']}:raise ValueError()
            if job['status'] in ('FAILED','CANCELLED'):
                if job['datasetId'] is not None or job['manifestSha256'] is not None:raise ValueError()
                continue
            if job['status']!='SUCCEEDED' or job['datasetId']!=identity or identity not in sources:raise ValueError()
            name=prefix+'prepared/manifest.json';manifest=inventory.get('preparedManifests',{}).get(name)
            if name not in files or files[name]['sha256']!=job['manifestSha256'] or not isinstance(manifest,dict):raise ValueError()
            row=sources[identity];source=row['manifest'];source=json.loads(source) if isinstance(source,str) else source
            if manifest.get('version')!=1 or manifest.get('format')!='PARQUET_NATIVE' or manifest.get('semanticsVersion')!='csv-v2' or manifest.get('verified') is not True or manifest.get('sourceSha256')!=job['sourceSha256'] or manifest.get('sourceBytes')!=job['sourceBytes'] or manifest.get('rowCount')!=row['rowCount'] or manifest.get('columns')!=source['nativeColumns'] or manifest.get('conversion')!=NATIVE_CONVERSION:raise ValueError()
            canonical=manifest['canonical']
            if canonical.get('path')!='canonical.csv' or canonical.get('sha256')!=row['sha256'] or files.get(prefix+'prepared/canonical.csv')!={'size':canonical['size'],'sha256':canonical['sha256']}:raise ValueError()
        except (ValueError,KeyError,TypeError,AttributeError):issues.append({'kind':'nativeImportMismatch','importId':identity})
    unknown={p.split('/')[1] for p in files if p.startswith('imports/') and len(p.split('/'))>2}-registered
    warnings.extend({'kind':'unregisteredImportDirectory','importId':identity} for identity in sorted(unknown))

def audit(project):
    project.offline()
    return compare(project.inventory(),volume_inventory(project.volume('backend','/data')))

def application_inventory(stream):
    result=scan_tar(stream)
    names=result['files']
    if 'backend.jar' not in names or 'engine/spark-engine-0.1.0.jar' not in names or not any(n.startswith('engine/dependency/') for n in names):
        raise StorageError('Application archive is incomplete')
    if any(n!='backend.jar' and not (n.startswith('engine/') and n.endswith('.jar')) for n in names):
        raise StorageError('Unexpected application archive files')
    return result

def backup(project,output):
    project.offline();output.mkdir(mode=0o700)
    metadata=project.inventory()
    point=recovery_point(project)
    validate_recovery_point(point,metadata['counts'])
    with (output/'database.sql').open('xb') as stream:
        os.chmod(output/'database.sql',0o600);project.mysql(output=stream,dump=True)
    with (output/'files.tar').open('xb') as stream:
        os.chmod(output/'files.tar',0o600);run(tar_command(project.volume('backend','/data')),output=stream,timeout=180)
    with (output/'application.tar').open('xb') as stream:
        os.chmod(output/'application.tar',0o600)
        run(['docker','cp',project.service('backend')+':/app/.','-'],output=stream,timeout=180)
    with (output/'application.tar').open('rb') as stream:application_inventory(stream)
    with (output/'files.tar').open('rb') as stream:report=compare(metadata,scan_tar(stream))
    save_json(output/'audit.json',report)
    if not report['ok']:raise StorageError('Integrity issues; incomplete backup preserved')
    manifest={'formatVersion':'sparkdq-backup-v1','createdAt':dt.datetime.now(dt.timezone.utc).isoformat(),
      'mysqlImage':MYSQL_IMAGE,'backendImageId':run(['docker','image','inspect','--format','{{.Id}}',BACKEND_IMAGE]).decode().strip(),
      'sourceContainerImageId':run(['docker','inspect','--format','{{.Image}}',project.service('backend')]).decode().strip(),
      'counts':metadata['counts'],'recoveryPoint':point,'artifacts':{n:digest(output/n) for n in ('database.sql','files.tar','application.tar','audit.json')}}
    save_json(output/'manifest.json',manifest);save_json(output/'COMPLETE',digest(output/'manifest.json'))
    return {'backup':str(output),'counts':manifest['counts'],'warnings':len(report['warnings'])}

def verify(folder):
    try:
        for name in ('COMPLETE','manifest.json','database.sql','files.tar','application.tar','audit.json'):
            p=folder/name
            if p.is_symlink() or not p.is_file():raise StorageError('Missing/non-regular backup file: '+name)
        if json.loads((folder/'COMPLETE').read_text())!=digest(folder/'manifest.json'):raise StorageError('Manifest checksum mismatch')
        manifest=json.loads((folder/'manifest.json').read_text())
        if not isinstance(manifest,dict) or manifest.get('formatVersion')!='sparkdq-backup-v1':raise StorageError('Unsupported backup format')
        if set(manifest.get('artifacts',{}))!={'database.sql','files.tar','application.tar','audit.json'}:raise StorageError('Invalid artifact set')
        for name,expected in manifest['artifacts'].items():
            if digest(folder/name)!=expected:raise StorageError('Checksum mismatch: '+name)
        with (folder/'files.tar').open('rb') as stream:scan_tar(stream)
        with (folder/'application.tar').open('rb') as stream:application_inventory(stream)
        if 'recoveryPoint' in manifest:validate_recovery_point(manifest['recoveryPoint'],manifest['counts'])
        if not json.loads((folder/'audit.json').read_text()).get('ok'):raise StorageError('Backup audit is not clean')
        return manifest
    except (OSError,ValueError,KeyError,TypeError,AttributeError,tarfile.TarError) as error:
        raise StorageError('Invalid/incomplete backup: '+type(error).__name__) from None

def restore(folder,destination):
    manifest=verify(folder)  # No Docker mutation before full validation.
    if manifest.get('mysqlImage')!=MYSQL_IMAGE:raise StorageError('Unsupported MySQL image')
    image=manifest.get('backendImageId','')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}',image):raise StorageError('Invalid backend image ID')
    for candidate in (image,MYSQL_IMAGE):run(['docker','image','inspect','--format','{{.Id}}',candidate])
    identity='spark-dq-restore-'+uuid.uuid4().hex[:12];destination.mkdir(mode=0o700)
    env=destination/'.env'
    with env.open('x') as stream:
        os.chmod(env,0o600);stream.write('MYSQL_PASSWORD='+secrets.token_hex(24)+'\nMYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\n')
    mysql_volume,files_volume,app_volume=identity+'-mysql',identity+'-files',identity+'-application'
    config=destination/'compose.yaml'
    config.write_text('name: '+identity+"""
services:
  mysql:
    image: """+MYSQL_IMAGE+"""
    environment:
      MYSQL_DATABASE: sparkdq
      MYSQL_USER: sparkdq
      MYSQL_PASSWORD: ${MYSQL_PASSWORD}
      MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD}
    volumes: [mysql-data:/var/lib/mysql]
    healthcheck:
      test: ["CMD-SHELL", "MYSQL_PWD=$$MYSQL_PASSWORD mysql -u sparkdq -D sparkdq -e 'SELECT 1' >/dev/null"]
      interval: 2s
      timeout: 5s
      retries: 50
  backend:
    image: """+image+"""
    environment:
      MYSQL_PASSWORD: ${MYSQL_PASSWORD}
      SPARK_LOCAL_IP: 127.0.0.1
    ports: ["127.0.0.1::8080"]
    volumes: [files:/data, application:/app:ro]
    depends_on:
      mysql: {condition: service_healthy}
volumes:
  mysql-data: {external: true, name: """+mysql_volume+"""}
  files: {external: true, name: """+files_volume+"""}
  application: {external: true, name: """+app_volume+"""}
""")
    # Explicit collision-free subnet: preserved experiments may exhaust Docker's default pool.
    from cluster_lab import network_plan
    networks=network_plan()
    config.write_text(config.read_text()+'\nnetworks:\n  default:\n    ipam:\n      config:\n        - subnet: '+networks['default']+'\n')
    os.chmod(config,0o600);project=Project(config,env)
    receipt={'formatVersion':'sparkdq-restore-v1','project':identity,'config':str(config),
      'mysqlVolume':mysql_volume,'filesVolume':files_volume,'applicationVolume':app_volume,'state':'preparing',
      'sourceManifestSha256':digest(folder/'manifest.json')['sha256']}
    save_json(destination/'receipt.json',receipt)
    for volume in (mysql_volume,files_volume,app_volume):
        if volume in run(['docker','volume','ls','--format','{{.Name}}']).decode().splitlines():
            raise StorageError('Refusing to reuse an existing volume')
        run(['docker','volume','create','--label','sparkdq.restore='+identity,volume])
    try:
        run(project.compose+['up','-d','mysql'])
        end=time.monotonic()+100
        while time.monotonic()<end:
            if run(['docker','inspect','--format','{{.State.Health.Status}}',project.service('mysql')]).strip()==b'healthy':break
            time.sleep(1)
        else:raise StorageError('Isolated MySQL health timeout')
        run(['docker','run','--rm','--network','none','--read-only','--user','0:0','--cap-drop','ALL',
          '--cap-add','CHOWN','--cap-add','DAC_OVERRIDE','--cap-add','FOWNER','--security-opt','no-new-privileges',
          '--mount','type=volume,source='+files_volume+',target=/data',
          '--mount','type=bind,source='+str(folder.resolve())+',target=/backup,readonly',
          '--entrypoint','tar',image,'-xf','/backup/files.tar','-C','/data','--same-owner'])
        run(['docker','run','--rm','--network','none','--read-only','--user','0:0','--cap-drop','ALL',
          '--cap-add','CHOWN','--cap-add','DAC_OVERRIDE','--cap-add','FOWNER','--security-opt','no-new-privileges',
          '--mount','type=volume,source='+app_volume+',target=/app-restore',
          '--mount','type=bind,source='+str(folder.resolve())+',target=/backup,readonly',
          '--entrypoint','tar',image,'-xf','/backup/application.tar','-C','/app-restore','--same-owner'])
        with (folder/'application.tar').open('rb') as stream:expected_app=application_inventory(stream)
        if volume_inventory(app_volume)!=expected_app:raise StorageError('Restored application files differ from backup')
        with (folder/'database.sql').open('rb') as stream:
            command=['docker','exec','-i',project.service('mysql'),'sh','-c',
              'export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; exec mysql --binary-mode --default-character-set=utf8mb4 --user=root --database="$MYSQL_DATABASE"']
            try:result=subprocess.run(command,stdin=stream,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=180)
            except subprocess.TimeoutExpired:raise StorageError('Isolated SQL import timeout') from None
            if result.returncode:raise StorageError('Isolated SQL import failed')
        restored=project.inventory();copied=volume_inventory(files_volume)
        with (folder/'files.tar').open('rb') as stream:expected_files=scan_tar(stream)
        if copied!=expected_files:raise StorageError('Restored file inventory differs from archive')
        report=compare(restored,copied)
        if restored['counts']!=manifest['counts'] or not report['ok']:raise StorageError('Isolated metadata/file verification failed')
        if 'recoveryPoint' in manifest:
            actual_point=recovery_point(project)
            if actual_point['sha256']!=manifest['recoveryPoint']['sha256']:raise StorageError('Restored business records differ from recovery point')
            save_json(destination/'recovery-point.json',actual_point)
        save_json(destination/'audit.json',report);run(project.compose+['up','-d','backend'])
        address=run(project.compose+['port','backend','8080']).decode().strip()
        if not re.fullmatch(r'127[.]0[.]0[.]1:[0-9]+',address):raise StorageError('Unexpected restore binding')
        base='http://'+address+'/api';end=time.monotonic()+90
        while time.monotonic()<end:
            try:
                with LOCAL.open(base+'/health',timeout=5) as response:
                    if json.load(response)['status']=='UP':break
            except (OSError,ValueError):pass
            time.sleep(1)
        else:raise StorageError('Isolated backend health timeout')
        receipt.update(state='verified',api=base,counts=restored['counts'],auditOk=True)
        save_json(destination/'verified.json',receipt);return receipt
    except Exception:
        with contextlib.suppress(StorageError):run(project.compose+['stop'],timeout=90)
        raise  # Preserve every new volume and partial receipt for diagnosis.

@contextlib.contextmanager
def lock():
    (ROOT/'.runtime').mkdir(exist_ok=True)
    with (ROOT/'.runtime/storage.lock').open('a') as handle:
        try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise StorageError('Another storage operation is running') from None
        yield

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path);parser.add_argument('--env',type=Path)
    commands=parser.add_subparsers(dest='command',required=True)
    p=commands.add_parser('audit');p.add_argument('--output',type=Path,required=True)
    p=commands.add_parser('backup');p.add_argument('--output',type=Path,required=True)
    p=commands.add_parser('verify');p.add_argument('backup',type=Path)
    p=commands.add_parser('restore');p.add_argument('backup',type=Path);p.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    # Resolve caller-relative paths before Docker commands switch to the repo cwd.
    for name in ('config','env','output','backup'):
        value=getattr(args,name,None)
        if value is not None:setattr(args,name,value.absolute())
    try:
        with lock():
            project=Project(args.config,args.env)
            if args.command=='audit':
                report=audit(project);save_json(args.output,report)
                result={'report':str(args.output),'ok':report['ok'],'issues':len(report['issues']),'warnings':len(report['warnings'])};code=0 if report['ok'] else 2
            elif args.command=='backup':result=backup(project,args.output);code=0
            elif args.command=='verify':result={'verified':True,'counts':verify(args.backup)['counts']};code=0
            else:result=restore(args.backup,args.output);code=0
        print(json.dumps(result,ensure_ascii=False));return code
    except (StorageError,OSError,tarfile.TarError) as error:
        print(json.dumps({'error':str(error),'type':type(error).__name__},ensure_ascii=False),file=sys.stderr);return 1



def validate_mysql_source(row,node,columns,metadata,inventory):
    jobs=[job for job in metadata.get('mysqlImports',[]) if job['id']==row['id']]
    if len(jobs)!=1:raise ValueError()
    job=jobs[0];prefix='mysql-imports/'+job['id']+'/snapshot/';files=inventory['files']
    request=job['request'];request=json.loads(request) if isinstance(request,str) else request
    if job['status']!='SUCCEEDED' or job['datasetId']!=row['id'] or node.get('version')!=1 or node.get('order')!='UNSPECIFIED' or node.get('importId')!=row['id'] or node.get('sourceId')!=request.get('sourceId') or node.get('columns')!=columns or node.get('rowCount')!=row['rowCount'] or node.get('canonicalSha256')!=row['sha256'] or node.get('snapshotManifestSha256')!=job['manifestSha256'] or job['sourceSha256']!=row['sha256']:raise ValueError()
    expected={'path':'canonical.csv','name':job['name'],'size':job['sourceBytes'],'sha256':job['sourceSha256'],'rows':row['rowCount']}
    if node.get('files')!=[expected] or type(job['sourceBytes']) is not int or not 0<job['sourceBytes']<=20*1024**2 or files.get(prefix+'canonical.csv')!={'size':job['sourceBytes'],'sha256':job['sourceSha256']}:raise ValueError()
    name=prefix+'manifest.json';snapshot=inventory.get('preparedManifests',{}).get(name)
    if name not in files or files[name]['sha256']!=job['manifestSha256'] or not isinstance(snapshot,dict):raise ValueError()
    if snapshot.get('isolation')!='REPEATABLE READ; WITH CONSISTENT SNAPSHOT; READ ONLY':raise ValueError()
    if snapshot.get('format')!='MYSQL_SNAPSHOT' or snapshot.get('version')!=1 or snapshot.get('canonicalSha256')!=row['sha256'] or snapshot.get('rows')!=row['rowCount'] or snapshot.get('bytes')!=job['sourceBytes'] or snapshot.get('schema')!=node.get('schema') or snapshot.get('table')!=node.get('table') or snapshot.get('mapping')!=node.get('mapping') or snapshot.get('types')!=node.get('types'):raise ValueError()
    if [entry.get('source') for entry in node['mapping']]!=request.get('columns') or [entry.get('target') for entry in node['mapping']]!=columns:raise ValueError()

def compare_mysql(metadata,inventory,issues,warnings):
    registered=set();sources={row['id']:row for row in metadata.get('sourceImports',[])}
    for job in metadata.get('mysqlImports',[]):
        identity=job['id'];registered.add(identity)
        try:
            if not re.fullmatch(r'[0-9a-f-]{36}',identity) or job['contractVersion']!=1:raise ValueError()
            if job['status'] in ('FAILED','CANCELLED'):
                if job['datasetId'] is not None or job['manifestSha256'] is not None:raise ValueError()
                continue
            if job['status']!='SUCCEEDED' or identity not in sources:raise ValueError()
            row=sources[identity];node=row['manifest'];node=json.loads(node) if isinstance(node,str) else node
            columns=row['columns'];columns=json.loads(columns) if isinstance(columns,str) else columns
            validate_mysql_source(row,node,columns,metadata,inventory)
        except (ValueError,KeyError,TypeError,AttributeError):issues.append({'kind':'mysqlSnapshotMismatch','importId':identity})
    unknown={p.split('/')[1] for p in inventory['files'] if p.startswith('mysql-imports/') and len(p.split('/'))>2}-registered
    warnings.extend({'kind':'unregisteredMysqlImportDirectory','importId':identity} for identity in sorted(unknown))


if __name__=='__main__':sys.exit(main())
