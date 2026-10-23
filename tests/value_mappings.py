#!/usr/bin/env python3
"""Real Spark mapping acceptance; only new synthetic data/versions are created."""
import argparse,collections,copy,csv,hashlib,io,json,time,uuid
from pathlib import Path
import e2e
ROOT=Path(__file__).resolve().parent.parent

def call(path,body,expected=200):
    code,result=e2e.http(path,json.dumps(body,ensure_ascii=False).encode(),{'Content-Type':'application/json'})
    assert code==expected,(code,result)
    return result
def finished(created):
    task=e2e.wait(created['id']);assert task['status']=='SUCCEEDED',task
    return task
def download(id):
    with e2e.LOCAL.open(e2e.BASE+'/datasets/'+id+'/download',timeout=30) as response:return response.read()
def rows(task):
    id=task['governance']['output_dataset_id'];raw=download(id)
    code,data=e2e.http('/datasets/'+id)
    assert code==200 and data['sha256']==hashlib.sha256(raw).hexdigest() and data['row_count']==task['total_rows']
    assert data['version']['governance_task_id']==task['id']
    return list(csv.reader(io.StringIO(raw.decode('utf-8-sig'),newline='')))[1:]
def metrics(task):
    return sorted([(r['type'],r['columns'],r.get('params',{}),r['metrics']) for r in task['rules']],key=lambda x:(x[0],x[1]))
def change(task,column):
    return next(x for x in task['governance']['audit']['transformations'] if x['column']==column and x['operation']=='VALUE_MAPPING')
def check_fixture(fixture):
    assert download(fixture['dataset']['id'])==bytes.fromhex(fixture['rawHex'])
    for task in fixture['tasks']:
        assert e2e.http('/tasks/'+task['id'])[1]==task
        if (task.get('governance') or {}).get('output_dataset_id'):
            raw=download(task['governance']['output_dataset_id'])
            assert hashlib.sha256(raw).hexdigest()==fixture['outputDigests'][task['id']]
    assert e2e.http('/tasks/'+fixture['reportTask']+'/report')[1]==fixture['report']

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base',default='http://127.0.0.1:18080/api')
    p.add_argument('--check',type=Path);args=p.parse_args();e2e.BASE=args.base
    if args.check:
        check_fixture(json.loads(args.check.read_text()));print('PASS immutable mapping policy/audit/version/CSV/report persistence');return
    literal='中文,"quoted"\nC:\\files\tend'
    source=[['1',' a ','start','1'],['1','B','keep','2'],['2','C','old','bad'],['3','KEEP','noop','3'],
      ['4','','empty','4'],['5','\\N','null','5'],['6','   ','spaces','6'],['7','a','case','7'],
      ['8','Z','unmatched','8'],['9','L','literal','9']]
    stream=io.StringIO(newline='');writer=csv.writer(stream,quoting=csv.QUOTE_ALL,lineterminator='\r\n')
    writer.writerow(['id','status','note','amount']);writer.writerows(source);raw=stream.getvalue().encode()
    code,dataset=e2e.upload(raw,'value-mapping-synthetic.csv');assert code==200
    rules=[{'type':'MISSING','columns':['status']},{'type':'MISSING','columns':['note']},
      {'type':'DUPLICATE','columns':['id']},{'type':'TYPE','columns':['amount'],'params':{'dataType':'INTEGER'}},
      {'type':'REGEX','columns':['status'],'params':{'pattern':'[A-Z]+'}}]
    parent=finished(call('/tasks',{'datasetId':dataset['id'],'rules':rules}))
    pair=lambda a,b:{'from':a,'to':b}
    invalid=[{'name':[pair('a','b')]},{'status':[pair('a','b'),pair('a','c')]},
      {'status':[pair(None,'b')]},{'status':[pair(1,'b')]},{'status':[pair('a',True)]},
      {'status':[{'from':'a','to':'b','extra':1}]},{'status':[pair('a','\\N')]},
      {'status':[pair('\\N','a')]},{'status':[pair('a','\0')]},{'status':[pair('a','x'*1001)]},
      {'status':[]},{'status':[pair(str(i),'x') for i in range(51)]}]
    total=e2e.http('/tasks/page')[1]['total']
    for mappings in invalid:call('/tasks/'+parent['id']+'/govern',{'policyVersion':3,'mappings':mappings},400)
    call('/tasks/'+parent['id']+'/govern',{'policyVersion':2,'mappings':{'status':[pair('a','b')]}},400)
    call('/tasks/'+parent['id']+'/govern',{'policyVersion':5,'mappings':{'status':[pair('a','b')]}},400)
    assert e2e.http('/tasks/page')[1]['total']==total,'Invalid requests queued work'
    print('PASS validation: strict strings/shape, budgets, duplicates, reserved marker and version',flush=True)

    exact=finished(call('/tasks/'+parent['id']+'/govern',{'mappings':{'status':[pair('A','B'),pair('B','C')]}}))
    expected=copy.deepcopy(source);expected[1][1]='C'
    assert collections.Counter(map(tuple,rows(exact)))==collections.Counter(map(tuple,expected))
    assert change(exact,'status')['matchedRows']==1 and change(exact,'status')['changedRows']==1
    pairs=[pair('A','B'),pair('B','C'),pair('C',''),pair('KEEP','KEEP'),pair('','BLANK'),pair('L',literal),pair('missing-filled','not-applied')]
    policy={'policyVersion':3,'normalizations':{'status':'TRIM_UPPER'},'fills':{'status':'missing-filled'},'mappings':{'status':pairs}}
    mapped=finished(call('/tasks/'+parent['id']+'/govern',policy))
    expected=copy.deepcopy(source)
    for row,value in zip(expected,['B','C','missing-filled','KEEP','BLANK','missing-filled','BLANK','B','Z',literal]):row[1]=value
    assert collections.Counter(map(tuple,rows(mapped)))==collections.Counter(map(tuple,expected))
    audit=change(mapped,'status')
    assert (audit['matchedRows'],audit['changedRows'])==(8,7),audit
    by={x['from']:x for x in audit['entries']}
    assert by['A']['matchedRows']==2 and by['B']['matchedRows']==1 and by['KEEP']['changedRows']==0
    assert by['']['matchedRows']==2 and by['missing-filled']['matchedRows']==0
    assert [(x['operation'],x['changedRows']) for x in mapped['governance']['audit']['transformations']]==[('TRIM_UPPER',3),('VALUE_MAPPING',7),('FILL_MISSING',2)]
    policy['mappings']['status'][0]['to']='edited-after-submit'
    assert e2e.http('/tasks/'+mapped['id'])[1]==mapped
    print('PASS simultaneous exact mapping, normalize/map/fill order, NULL, empty, no-op, literal CSV and immutable audit',flush=True)

    combined=copy.deepcopy(mapped['governance']['policy'])
    combined['mappings']['id']=[pair('9','8')];combined['mappings']['note']=[pair('old','')]
    combined.update(dropMissing=True,deduplicate=True)
    composed=finished(call('/tasks/'+parent['id']+'/govern',combined))
    actual=rows(composed);assert composed['total_rows']==7 and len({x[0] for x in actual})==7
    audit=composed['governance']['audit']
    assert audit['droppedMissingRows']==1 and audit['deduplicatedRows']==2,audit
    assert change(composed,'id')['changedRows']==1 and change(composed,'note')['changedRows']==1
    assert next(r['metrics']['invalidCount'] for r in composed['rules'] if r['type']=='TYPE')==0
    noop=finished(call('/tasks/'+parent['id']+'/govern',{'mappings':{'status':[pair('absent','unused')]}}))
    assert collections.Counter(map(tuple,rows(noop)))==collections.Counter(map(tuple,source))
    assert change(noop,'status')['matchedRows']==0 and change(noop,'status')['changedRows']==0
    zero=finished(call('/tasks/'+parent['id']+'/govern',{'policyVersion':3,'dropMissing':True,
      'mappings':{'status':[pair(x,'') for x in sorted({row[1] for row in source if row[1]!='\\N'})]}}))
    assert zero['total_rows']==0 and rows(zero)==[] and zero['governance']['audit']['droppedMissingRows']==10
    rerun=finished(call('/tasks/'+mapped['id']+'/rerun',{}));assert metrics(rerun)==metrics(mapped)
    report=e2e.http('/tasks/'+composed['id']+'/report')[1]
    assert report['task']['governance']==composed['governance'] and len(report['comparison'])==5
    assert download(dataset['id'])==raw and e2e.http('/tasks/'+parent['id'])[1]==parent
    fixture={'dataset':dataset,'rawHex':raw.hex(),'tasks':[parent,exact,mapped,composed,noop,zero,rerun],
      'outputDigests':{t['id']:hashlib.sha256(download(t['governance']['output_dataset_id'])).hexdigest() for t in [exact,mapped,composed,noop,zero]},
      'reportTask':composed['id'],'report':report}
    path=ROOT/'.runtime'/('value-mappings-'+uuid.uuid4().hex[:12]+'.json');path.write_text(json.dumps(fixture,ensure_ascii=False,indent=2))
    check_fixture(fixture)
    print(json.dumps({'ok':True,'fixture':str(path),'mappedTask':mapped['id'],'composedTask':composed['id'],
      'zeroTask':zero['id'],'rerunTask':rerun['id'],'audit':mapped['governance']['audit']},ensure_ascii=False),flush=True)
if __name__=='__main__':main()
