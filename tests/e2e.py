#!/usr/bin/env python3
"""HTTP end-to-end checks against real MySQL + independent Spark JVM; stdlib only."""
import json, urllib.request, urllib.error, uuid, time, sys
from pathlib import Path
BASE='http://127.0.0.1:18080/api'
# Tests only address the local service; inherited proxies must not intercept it.
LOCAL=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def http(path,body=None,headers=None,method=None):
    req=urllib.request.Request(BASE+path,data=body,headers=headers or {},method=method)
    try:
        with LOCAL.open(req,timeout=30) as res:return res.status,json.load(res)
    except urllib.error.HTTPError as e:return e.code,json.load(e)
def upload(data,name='test.csv'):
    boundary=uuid.uuid4().hex
    payload=(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\nContent-Type: text/csv\r\n\r\n').encode()+data+f'\r\n--{boundary}--\r\n'.encode()
    return http('/datasets',payload,{'Content-Type':'multipart/form-data; boundary='+boundary})
def create(dataset,fields,keys,status=200):
    code,res=http('/tasks',json.dumps({'datasetId':dataset,'fields':fields,'keys':keys}).encode(),{'Content-Type':'application/json'})
    assert code==status,(code,res)
    return res
def wait(id):
    end=time.monotonic()+310
    while time.monotonic()<end:
        code,task=http('/tasks/'+id)
        assert code==200,(code,task)
        if task['status'] in ('SUCCEEDED','FAILED'):return task
        time.sleep(1)
    raise AssertionError('Timeout '+id)
def uploads():
    invalid=[(b'', 'empty.csv'),(b'id,id\n1,2\n','duplicate.csv'),(b'id,name\n1\n','ragged.csv'),(b'id,name\n','header.csv'),(b'id,name\n1,"broken\n','bad.csv'),(b'id,name\n1,\xff\n','encoding.csv'),(b'id,name\n1,ok\n','data.txt'),(b'__dq_group_size\n1\n','reserved.csv')]
    for data,name in invalid:
        code,res=upload(data,name);assert code==400,(name,code,res)
    code,bom=upload(b'\xef\xbb\xbfid,name\r\n1,"hello\nworld"\r\n','multiline.csv')
    assert code==200 and bom['row_count']==1 and bom['preview'][0][1]=='hello\nworld',(code,bom)
    code,d=upload(Path('datasets/known-issues.csv').read_bytes(),'known-issues.csv')
    assert code==200 and d['row_count']==7 and d['columns']==['id','name','email','region'],(code,d)
    assert len(d['sha256'])==64 and len(d['id'])==36
    assert http('/datasets/'+d['id'])[1]['preview']==d['preview']
    print('PASS uploads: invalid formats/UTF-8/header/row widths/extensions, BOM + quoted newline, registration + preview',flush=True)
    return d
def full():
    d=uploads()
    for fields,keys in [([],['id']),(['missing'],['id']),(['id'],[]),(['id'],['missing']),(['id','id'],['id'])]:create(d['id'],fields,keys,400)
    t=wait(create(d['id'],['name','email'],['id'])['id']);assert t['status']=='SUCCEEDED',t
    assert t['total_rows']==7 and t['spark_version']=='3.5.8' and t['duration_ms']>0,t
    assert len({r['id'] for r in t['rules']})==3 and all(r['result_id'] for r in t['rules'])
    by={r['type']+':'+','.join(r['columns']):r for r in t['rules']}
    assert by['MISSING:name']['metrics']=={'nullCount':1,'emptyCount':1,'whitespaceCount':1,'blankCount':3},t
    assert by['MISSING:email']['metrics']=={'nullCount':2,'emptyCount':0,'whitespaceCount':0,'blankCount':2},t
    assert by['DUPLICATE:id']['metrics']=={'duplicateGroups':1,'duplicateRows':2,'excessRows':1},t
    for key,expected in [('MISSING:name',3),('MISSING:email',2),('DUPLICATE:id',2)]:
        code,samples=http(f"/tasks/{t['id']}/rules/{by[key]['id']}/preview");assert code==200 and len(samples)==expected,(key,code,samples)
    assert http(f"/tasks/{t['id']}/rules/{uuid.uuid4()}/preview")[0]==404
    print('PASS real Spark: seven records, missing/empty/whitespace metrics, duplicates 1 group / 2 rows / 1 excess, independent rule/result IDs + anomaly previews',flush=True)
    code,data=upload(b'id,name,region\n1,A,x\n1,A,y\n2,B,z\n2,B,z\n\\N,C,q\n\\N,D,q\n','composite.csv');assert code==200,data
    composite=wait(create(data['id'],['name'],['id','region'])['id']);assert composite['status']=='SUCCEEDED',composite
    duplicate=next(rule for rule in composite['rules'] if rule['type']=='DUPLICATE')
    assert duplicate['metrics']=={'duplicateGroups':2,'duplicateRows':4,'excessRows':2},composite
    print('PASS composite business keys + null keys grouped together',flush=True)
    code,clean=upload(b'id,name\n1,A\n2,B\n','clean.csv');assert code==200,clean
    clean_task=wait(create(clean['id'],['name'],['id'])['id']);assert clean_task['status']=='SUCCEEDED',clean_task
    for rule in clean_task['rules']:
        assert all(value==0 for value in rule['metrics'].values()),rule
        assert http(f"/tasks/{clean_task['id']}/rules/{rule['id']}/preview")[1]==[]
    print('PASS clean dataset: zero metrics and empty anomaly files',flush=True)
    # Hold durable IDs for restart and failure tests; ignored runtime file.
    Path('.runtime/e2e.json').write_text(json.dumps({'dataset':d,'task':t,'cleanDataset':clean,'cleanTask':clean_task}))
    print(json.dumps({'taskId':t['id'],'totalRows':t['total_rows'],'durationMs':t['duration_ms'],'compositeTask':composite['id'],'cleanTask':clean_task['id']},ensure_ascii=False),flush=True)
if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('base_url',nargs='?',help='Local API URL (legacy positional form)')
    parser.add_argument('--base',help='Explicit local API URL')
    parser.add_argument('--uploads-only',action='store_true')
    args=parser.parse_args()
    if args.base and args.base_url:parser.error('Specify API URL once')
    BASE=args.base or args.base_url or BASE
    if args.uploads_only:uploads()
    else:full()
