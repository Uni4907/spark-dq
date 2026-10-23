from e2e import upload,create,wait,http

fixtures=[
 ('case-sensitive.csv',b'id,ID\n1,A\n2,\\N\n',['ID'],['id']),
 ('count-key.csv',b'count,name\n1,A\n1,B\n2,C\n',['name'],['count']),
 ('blank-lines.csv',b'value\n\n""\n   \n\\N\nx\n',['value'],['value']),
 ('multiline-spark.csv',b'\xef\xbb\xbfid,name\r\n1,"hello\nworld"\r\n2,\\N\r\n',['name'],['id'])
]
for name,raw,fields,keys in fixtures:
 code,data=upload(raw,name);assert code==200,(code,data)
 task=wait(create(data['id'],fields,keys)['id'])
 print(name,'registered',data['row_count'],'spark',task['total_rows'],'status',task['status'],flush=True)
 assert task['status']=='SUCCEEDED',task
 assert task['total_rows']==data['row_count'],(data,task)
 if name=='count-key.csv':assert next(r['metrics'] for r in task['rules'] if r['type']=='DUPLICATE')['duplicateRows']==2
 if name=='multiline-spark.csv':assert next(r['metrics'] for r in task['rules'] if r['type']=='MISSING')['nullCount']==1
print('PASS edge cases: count business key, blank physical lines, BOM and quoted newlines in real Spark')
