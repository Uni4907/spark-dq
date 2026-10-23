"""Exact JSON decimal boundaries must not be rounded by the API."""
from e2e import http,upload,wait
import json,uuid
maximum='9999999999999999999999999999.1234567890'
code,data=upload(('value\n'+maximum+'\n9999999999999999999999999999.1234567891\n').encode(),'exact-decimals.csv')
assert code==200,data
body='{"name":"Exact decimal '+uuid.uuid4().hex[:8]+'","rules":[{"type":"RANGE","columns":["value"],"params":{"max":'+maximum+'}}]}'
code,template=http('/rule-templates',body.encode(),{'Content-Type':'application/json'})
assert code==200,(code,template)
assert template['rules'][0]['params']['max']=='9999999999999999999999999999.123456789',template
code,task=http('/tasks',json.dumps({'datasetId':data['id'],'templateId':template['template_id']}).encode(),{'Content-Type':'application/json'})
assert code==200,(code,task)
task=wait(task['id']);assert task['status']=='SUCCEEDED',task
assert task['rules'][0]['metrics']=={'nonNumericCount':0,'invalidCount':1,'checkedCount':2,'skippedBlankCount':0},task
assert task['rules'][0]['params']==template['rules'][0]['params']
print('PASS exact JSON boundary: 28 integer/10 decimal digits retained; inclusive maximum and next 0.0000000001 value distinguished by Spark',flush=True)
