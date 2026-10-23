"""Read-only task/parameter/template assertions after backend recreation or restart."""
from e2e import http
from pathlib import Path
import json
fixture=json.loads(Path('.runtime/rule_templates.json').read_text())
for key in ('source','latest','rerun','governed'):
    expected=fixture[key];code,actual=http('/tasks/'+expected['id'])
    assert code==200 and actual==expected,(key,actual)
    if key in ('rerun','governed'):
        report=http('/tasks/'+expected['id']+'/report')[1]
        assert len(report['comparison'])==8
        assert all('params' in row for row in report['comparison'])
tid=fixture['template']['template_id']
assert http('/rule-templates/'+tid+'?revision=1')[1]['rules']==fixture['template']['rules']
archived=http('/rule-templates/'+tid)[1]
assert archived==fixture['archivedTemplate']
active=fixture['activeTemplate']
assert http('/rule-templates/'+active['template_id'])[1]==active
print('PASS rules persistence: immutable template revisions/archive, task parameters/provenance, rerun/governance results and 8-rule reports',flush=True)
