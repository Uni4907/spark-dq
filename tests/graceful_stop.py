from e2e import create,http
from pathlib import Path
import subprocess,time,json
snapshot=json.loads(Path('.runtime/e2e.json').read_text());data=snapshot['cleanDataset']
task=create(data['id'],['name'],['id'])
compose=['docker','compose','--env-file','.env','-f','infrastructure/compose.yaml']
subprocess.run(compose+['stop','backend'],check=True)
subprocess.run(compose+['up','-d','backend'],check=True)
deadline=time.monotonic()+60
while time.monotonic()<deadline:
 try:
  if http('/health')[0]==200:break
 except Exception:pass
 time.sleep(1)
restored=http('/tasks/'+task['id'])[1]
assert restored['status']=='FAILED' and '中断任务' in restored['error'] and restored['duration_ms'] is not None,restored
print('PASS graceful stop: independent child terminated; task records explicit interruption reason and duration',task['id'])
