"""Create a fresh, retained MySQL fixture without touching the source project."""
import sys,os,secrets,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT/'infrastructure'))
import storage as st

def create_fixture():
    folder=ROOT/'.runtime'/('migrations-'+uuid.uuid4().hex[:12]);folder.mkdir(mode=0o700)
    identity='spark-dq-migrations-'+uuid.uuid4().hex[:12]
    env=folder/'.env'
    env.write_text('MYSQL_PASSWORD='+secrets.token_hex(24)+'\nMYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\n')
    os.chmod(env,0o600)
    config=folder/'compose.yaml'
    config.write_text((ROOT/'tests/fixtures/migrations-compose.yaml').read_text().replace('__PROJECT__',identity))
    p=st.Project(config,env);st.run(p.compose+['up','-d','mysql'])
    end=time.monotonic()+110
    while time.monotonic()<end:
        try:
            p.mysql('SELECT 1;');break
        except st.StorageError:time.sleep(2)
    else:raise RuntimeError('Isolated MySQL timeout')
    p.mysql((ROOT/'tests/fixtures/legacy-schema.sql').read_text())
    metadata=p.mysql((ROOT/'backend/src/main/resources/db/legacy-metadata.sql').read_text()).decode()
    assert metadata==(ROOT/'backend/src/main/resources/db/legacy-metadata.txt').read_text(), 'Reference schema metadata differs'
    p.mysql("CREATE DATABASE emptydb; GRANT ALL ON emptydb.* TO 'sparkdq'@'%';")
    st.save_json(folder/'setup.json',{'folder':str(folder),'project':identity,'metadataEntries':len(metadata.splitlines())})
    return folder
