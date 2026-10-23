"""Read-only assertions for governance metadata/files after service restart."""
from e2e import http,LOCAL,BASE
from pathlib import Path
import json,hashlib
fixtures=json.loads(Path('.runtime/governance.json').read_text())
for name,expected in fixtures.items():
    code,actual=http('/tasks/'+expected['id'])
    assert code==200 and actual==expected,(name,code,actual)
    report=http('/tasks/'+actual['id']+'/report')[1]
    if actual['governance']:
        dataset=report['outputDataset']
        with LOCAL.open(BASE+'/datasets/'+dataset['id']+'/download',timeout=20) as res: raw=res.read()
        assert hashlib.sha256(raw).hexdigest()==dataset['sha256']
        assert dataset['version']['governance_task_id']==actual['id']
        assert report['comparison']
print('PASS governance restart persistence: policy, parent versions, output bytes/checksums, recheck results and reports',flush=True)
