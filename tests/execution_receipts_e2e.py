"""Read-only API receipt verification for an existing native_imports_e2e fixture."""
import argparse,json,uuid,re
from pathlib import Path
import e2e

def check(fixture,historical_id=None):
 attempts=set();apps=set()
 for expected in fixture['tasks']:
  code,task=e2e.http('/tasks/'+expected['id']);assert code==200 and task==expected
  proof=task['execution'];assert proof['evidenceState']=='DRIVER_ENDED'
  submitted,started,ended=(proof[name] for name in ('submitted','started','ended'))
  assert submitted['taskId']==task['id'] and str(uuid.UUID(submitted['attemptId']))==submitted['attemptId']
  assert re.fullmatch('[0-9a-f]{64}',submitted['requestSha256'])
  for key in ('taskId','attemptId','requestSha256','sparkMaster'):assert submitted[key]==started[key]==ended[key]
  assert started['applicationId']==ended['applicationId'] and ended['driverOutcome']=='COMPLETED' and ended['stopReturned'] is True
  assert submitted['attemptId'] not in attempts and started['applicationId'] not in apps
  attempts.add(submitted['attemptId']);apps.add(started['applicationId'])
 code,report=e2e.http('/tasks/'+fixture['reportTask']+'/report');assert code==200 and report==fixture['report']
 assert report['task']['execution']['evidenceState']=='DRIVER_ENDED'
 if historical_id:
  code,task=e2e.http('/tasks/'+historical_id);assert code==200 and task['execution']=={'evidenceState':'ABSENT'}
 return {'ok':True,'tasks':len(attempts),'uniqueAttempts':len(attempts),'uniqueApplications':len(apps),'reportPreserved':True,'historicalChecked':bool(historical_id)}

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--base',required=True);p.add_argument('--fixture',type=Path,required=True);p.add_argument('--historical-id');a=p.parse_args();e2e.BASE=a.base;print(json.dumps(check(json.loads(a.fixture.read_text()),a.historical_id)),flush=True)
