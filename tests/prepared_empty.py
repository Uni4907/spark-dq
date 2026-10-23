"""Real empty-version regression, kept separate from timed nonempty measurements."""
import argparse,csv,json,os,shutil,sys,uuid,fcntl,signal
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'infrastructure'))
import prepared_inputs as p
import benchmark as b
import storage as st
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',default='spark-dq-backend:prepared');args=parser.parse_args()
    def interrupted(signum,frame):raise InterruptedError('Empty-version acceptance interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    with (st.ROOT/'.runtime/benchmark.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        b.source_idle();before=b.source_snapshot();session=uuid.uuid4().hex
        root=st.ROOT/'.runtime'/('prepared-empty-'+session[:12]);root.mkdir(mode=0o700)
        inputs=root/'inputs';inputs.mkdir(mode=0o755);(root/'cases').mkdir()
        with (inputs/'empty.csv').open('x',encoding='utf-8',newline='') as stream:csv.writer(stream).writerow(b.COLUMNS)
        os.chmod(inputs/'empty.csv',0o444)
        for kind in ('CSV','PARQUET'):
            folder=root/'cases'/('prepare-'+kind)
            p.worker(folder,inputs,{'input':'/benchmark/input/empty.csv','output':'/benchmark/output/prepared','format':kind,'partitions':4},
              args.image,session,prepare=True,timeout=120)
            shutil.copytree(folder/'prepared',inputs/kind)
        records=[]
        for kind in ('CSV_SINGLE','CSV','PARQUET'):
            for profile in ('extended','governance'):
                folder=root/'cases'/(kind+'-'+profile)
                payload={'taskId':folder.name,'output':'/benchmark/output/result','semanticsVersion':'csv-v2','rules':b.rules(profile)}
                if profile=='governance':payload['governance']=b.policy()
                if kind=='CSV_SINGLE':payload['input']='/benchmark/input/empty.csv'
                else:payload.update(inputManifest='/benchmark/input/'+kind+'/manifest.json',inputManifestSha256=st.digest(inputs/kind/'manifest.json')['sha256'])
                p.worker(folder,inputs,payload,args.image,session,timeout=120)
                actual=json.loads((folder/'result/result.json').read_text())
                b.validate_result(folder,b.oracle(0,profile),actual['execution']['inputPartitions'])
                assert actual['totalRows']==0;records.append(kind+'/'+profile)
                print('PASS empty '+records[-1],flush=True)
        assert before==b.source_snapshot()
        st.save_json(root/'verified.json',{'ok':True,'cases':records,'sourceUnchanged':True})
        print(json.dumps({'ok':True,'output':str(root),'cases':len(records)}),flush=True)
if __name__=='__main__':main()
