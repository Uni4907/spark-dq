#!/usr/bin/env python3
from pathlib import Path
import secrets,os
path=Path(__file__).resolve().parent.parent/'.env'
try:
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
except FileExistsError:
    print('.env already exists; preserved')
else:
    with os.fdopen(fd,'w') as f:f.write('MYSQL_PASSWORD='+secrets.token_hex(24)+'\nMYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\n')
    print('Created local random passwords in .env (not printed)')
