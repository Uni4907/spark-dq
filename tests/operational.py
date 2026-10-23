from e2e import upload,create,wait
from pathlib import Path
import subprocess,uuid,hashlib

probe=Path('.runtime')/('env-check-'+uuid.uuid4().hex)/'infrastructure'
probe.mkdir(parents=True)
script=probe/'init_env.py';script.write_text(Path('infrastructure/init_env.py').read_text())
subprocess.run(['python3',str(script)],check=True)
env=probe.parent/'.env'
assert len(env.read_text().splitlines())==2,'Generated .env must contain two separate lines'
before=env.read_bytes();subprocess.run(['python3',str(script)],check=True)
assert env.read_bytes()==before and (env.stat().st_mode&0o777)==0o600
print('PASS .env creation: two random values, mode 600, repeated init preserves bytes',flush=True)
code,result=upload(b'id,name\n'+b'x'*(20*1024*1024),'oversize.csv')
assert code==413,(code,result)
print('PASS upload limit: oversized multipart rejected with HTTP 413',flush=True)
raw='id,name,email\n'+''.join(f'{i//2},User{i},'+('' if i%100==0 else f'u{i}@example.test')+'\n' for i in range(10000))
code,data=upload(raw.encode(),'synthetic-10000.csv');assert code==200 and data['row_count']==10000,(code,data)
task=wait(create(data['id'],['email'],['id'])['id']);assert task['status']=='SUCCEEDED' and task['total_rows']==10000,task
metrics={r['type']:r['metrics'] for r in task['rules']}
assert metrics['MISSING']['blankCount']==100 and metrics['DUPLICATE']=={'duplicateGroups':5000,'duplicateRows':10000,'excessRows':5000},task
print(f"PASS 10,000 synthetic rows: 100 missing emails / 5,000 duplicate groups / 10,000 member rows / 5,000 excess; task {task['id']}, {task['duration_ms']} ms",flush=True)
