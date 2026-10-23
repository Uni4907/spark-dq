#!/usr/bin/env python3
"""Offline bounded native Parquet import preparation. No HTTP upload or database registration."""
import argparse,csv,hashlib,json,os,shutil,signal,sys,uuid
from pathlib import Path
import prepared_inputs as p
import storage as st
MAX_BYTES=20*1024**2
def _verify(root,complete=True):
 root=root.absolute()
 def regular(path,limit):
  if path.is_symlink() or not path.is_file() or path.resolve()!=path or path.stat().st_size>limit:raise st.StorageError('Missing/unsafe/oversized native import artifact')
 source=root/'inputs/source.parquet';manifest=root/'worker/imported/manifest.json';canonical=root/'worker/imported/canonical.csv';receipt=root/'source.json'
 for path,limit in [(source,MAX_BYTES),(manifest,1048576),(canonical,MAX_BYTES),(receipt,16384)]:regular(path,limit)
 original=json.loads(receipt.read_text());node=json.loads(manifest.read_text())
 if original['sha256']!=st.digest(source)['sha256'] or original['size']!=source.stat().st_size:raise st.StorageError('Frozen native source fingerprint changed')
 if type(node.get('version')) is not int or node['version']!=1 or node.get('format')!='PARQUET_NATIVE' or node.get('semanticsVersion')!='csv-v2' or node.get('verified') is not True:raise st.StorageError('Invalid native import manifest')
 if node.get('sourceSha256')!=original['sha256'] or node.get('sourceBytes')!=original['size']:raise st.StorageError('Native manifest/source mismatch')
 expected=node.get('canonical',{})
 if expected.get('path')!='canonical.csv' or {'sha256':expected.get('sha256'),'size':expected.get('size')}!=st.digest(canonical):raise st.StorageError('Canonical CSV checksum mismatch')
 descriptors=node.get('columns',[]);names=[f['name'] for f in descriptors]
 import re
 if not 1<=len(names)<=100 or len(set(names))!=len(names) or any(not re.fullmatch('[A-Za-z][A-Za-z0-9_]{0,63}',n) for n in names):raise st.StorageError('Invalid native fields')
 for field in descriptors:
  if not re.fullmatch(r'(string|boolean|tinyint|smallint|int|bigint|date|decimal\([0-9]+,[0-9]+\))',field['type']):raise st.StorageError('Unsupported native source type')
 if node.get('conversion')!={'version':1,'timezone':'UTC','decimal':'PLAIN_WITH_SCALE','integer':'BASE10','boolean':'LOWERCASE','date':'GREGORIAN_1582_10_15_TO_9999','string':'STRICT_UTF8','order':'UNSPECIFIED'}:raise st.StorageError('Unknown normalization contract')
 with canonical.open(encoding='utf-8',newline='') as stream:
  reader=csv.reader(stream,strict=True);header=next(reader,None);count=0
  if header!=names:raise st.StorageError('Canonical CSV column order mismatch')
  for row in reader:
   count+=1
   if count>1_000_000 or len(row)!=len(names) or any('\0' in value or len(value.encode('utf-16-le'))//2>65536 for value in row):raise st.StorageError('Canonical row/field limit exceeded')
 if type(node.get('rowCount')) is not int or count<1 or count!=node['rowCount']:raise st.StorageError('Canonical row count mismatch')
 result={'verified':True,'rows':count,'sourceSha256':original['sha256'],'manifestSha256':st.digest(manifest)['sha256'],'canonicalSha256':st.digest(canonical)['sha256']}
 if complete:
  regular(root/'COMPLETE',16384)
  if json.loads((root/'COMPLETE').read_text())!=result:raise st.StorageError('Native import completion marker mismatch')
 return result
def verify(root,complete=True):
 try:return _verify(root,complete)
 except (ValueError,KeyError,TypeError,AttributeError,UnicodeError,StopIteration,csv.Error) as error:raise st.StorageError('Invalid native import metadata or CSV: '+type(error).__name__) from None
def prepare(source,output,sha,image,capacity):
 source=source.absolute();output=output.absolute()
 if source.is_symlink() or not source.is_file() or source.resolve()!=source or not source.name.lower().endswith('.parquet') or not 12<=source.stat().st_size<=MAX_BYTES:raise st.StorageError('Source must be a local non-link Parquet file of 12 bytes..20MiB')
 output.resolve().relative_to(st.ROOT/'.runtime')
 if output.exists() or not output.parent.is_dir():raise st.StorageError('Output must be new under project .runtime')
 if not capacity.get('known') or capacity.get('freeBytes',0)<5*1024**3 or shutil.disk_usage(output.parent).free<5*1024**3:raise st.StorageError('Known host and Linux reserve of 5GiB required')
 if not __import__('re').fullmatch('[0-9a-f]{64}',sha) or st.digest(source)['sha256']!=sha:raise st.StorageError('Expected source SHA-256 mismatch')
 image=st.run(['docker','image','inspect','--format','{{.Id}}',image]).decode().strip()
 output.mkdir(mode=0o700);inputs=output/'inputs';inputs.mkdir(mode=0o755)
 # Freeze one file only. Other host files are never mounted into the converter.
 target=inputs/'source.parquet'
 with source.open('rb') as inp,target.open('xb') as out:
  size=0
  for chunk in iter(lambda:inp.read(8192),b''):
   size+=len(chunk)
   if size>MAX_BYTES:raise st.StorageError('Native source grew beyond20MiB')
   out.write(chunk)
 if st.digest(target)['sha256']!=sha or st.digest(source)['sha256']!=sha:raise st.StorageError('Source changed while freezing import')
 os.chmod(target,0o444)
 st.save_json(output/'source.json',dict(st.digest(target),originalName=source.name))
 st.save_json(output/'environment.json',{'image':image,'master':'local[2]','heap':'1g','containerMemoryBytes':2*1024**3,'timeoutSeconds':300,'capacity':capacity,'sourceBytes':target.stat().st_size})
 measured=p.worker(output/'worker',inputs,{'input':'/benchmark/input/source.parquet','output':'/benchmark/output/imported','sourceSha256':sha},image,uuid.uuid4().hex,import_parquet=True,timeout=300)
 proof=verify(output,complete=False);st.save_json(output/'verified.json',dict(proof,**measured));st.save_json(output/'COMPLETE',proof)
 verify(output);return dict(proof,output=str(output))
def main():
 parser=argparse.ArgumentParser(description=__doc__);commands=parser.add_subparsers(dest='command',required=True)
 c=commands.add_parser('prepare');c.add_argument('--input',type=Path,required=True);c.add_argument('--output',type=Path,required=True);c.add_argument('--sha256',required=True);c.add_argument('--image',default='spark-dq-backend:native-parquet');c.add_argument('--host-capacity',type=Path,required=True)
 c=commands.add_parser('verify');c.add_argument('output',type=Path)
 args=parser.parse_args()
 def interrupted(signum,frame):raise InterruptedError('Native import interrupted; evidence retained')
 signal.signal(signal.SIGTERM,interrupted)
 try:
  result=verify(args.output) if args.command=='verify' else prepare(args.input,args.output,args.sha256,args.image,json.loads(args.host_capacity.read_text(encoding='utf-8-sig')))
  print(json.dumps(result,ensure_ascii=False));return 0
 except (st.StorageError,OSError,ValueError,KeyError,TypeError,csv.Error,InterruptedError) as error:
  print(json.dumps({'error':str(error)},ensure_ascii=False),file=sys.stderr);return 1
if __name__=='__main__':sys.exit(main())
