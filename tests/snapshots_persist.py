"""Read-only verification of fixtures after a backend restart."""
from e2e import http
from pathlib import Path
import json
fixtures=json.loads(Path('.runtime/rerun.json').read_text())
for name, expected in fixtures.items():
    code, actual=http('/tasks/'+expected['id'])
    assert code==200 and actual==expected,(name,code,actual)
    for rule in actual['rules']:
        code, rows=http('/tasks/'+actual['id']+'/rules/'+rule['id']+'/preview')
        assert code==200,(code,rows)
        if name=='governed': assert rows==[],rows
print('PASS backend restart: source, reruns, snapshots, lineage, metrics and anomaly previews persisted',flush=True)
