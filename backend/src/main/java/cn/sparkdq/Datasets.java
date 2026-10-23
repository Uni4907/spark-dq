package cn.sparkdq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.nio.charset.*;
import java.nio.file.*;
import java.io.*;
import java.security.*;
import java.util.*;

@Service
public class Datasets {
 static final Path ROOT=Path.of("/data");
 final JdbcTemplate db; final ObjectMapper json;
 public Datasets(JdbcTemplate db,ObjectMapper json){this.db=db;this.json=json;}
 static ResponseStatusException bad(String message){return new ResponseStatusException(HttpStatus.BAD_REQUEST,message);}
 static String id(){return UUID.randomUUID().toString();}
 public Map<String,Object> one(String id){
  var rows=db.queryForList("select * from datasets where id=?",id);
  if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"数据集不存在");
  var row=rows.get(0); Object sources=row.remove("source_manifest_json");if(sources!=null)row.put("source",decode(sources)); row.put("columns",decode(row.remove("columns_json")));row.put("preview",CsvLimitsReader.previewRows(decode(row.remove("preview_json"))));row.put("previewCellLimit",CsvLimitsReader.PREVIEW);
  var versions=db.queryForList("select parent_dataset_id,governance_task_id,created_at from dataset_versions where dataset_id=?",id);
  row.put("version",versions.isEmpty()?null:versions.get(0));
  row.put("derivedVersions",db.queryForList("select v.dataset_id,v.governance_task_id,v.created_at,d.name,d.row_count from dataset_versions v join datasets d on d.id=v.dataset_id where v.parent_dataset_id=? order by v.created_at",id));return row;
 }
 Path sourceFile(String identity,int ordinal)throws Exception{
  var data=one(identity);
  if(!(data.get("source") instanceof Map<?,?> manifest)||!(manifest.get("files") instanceof List<?> files)||ordinal<1||ordinal>files.size())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"来源分片不存在");
  var file=(Map<?,?>)files.get(ordinal-1);String expected=String.format(Locale.ROOT,"%03d.csv",ordinal);
  boolean nativeImport="PARQUET_NATIVE".equals(manifest.get("format"));
  if(nativeImport){
   if(ordinal!=1||!data.get("id").equals(manifest.get("importId")))throw new ResponseStatusException(HttpStatus.CONFLICT,"原文件来源不匹配");
   expected="source.parquet";
  }
  boolean controlled="CONTROLLED_CSV".equals(manifest.get("format"));
  if(controlled)expected="canonical.csv";
  boolean mysql="MYSQL_SNAPSHOT".equals(manifest.get("format"));
  if(mysql){
   if(ordinal!=1||!data.get("id").equals(manifest.get("importId")))throw new ResponseStatusException(HttpStatus.CONFLICT,"业务快照来源不匹配");
   expected="canonical.csv";
  }
  Path path=controlled?ROOT.resolve("raw/"+data.get("id")+".csv").toAbsolutePath().normalize():mysql?ROOT.resolve("mysql-imports/"+data.get("id")+"/snapshot/canonical.csv").toAbsolutePath().normalize():ROOT.resolve((nativeImport?"imports/":"sources/")+data.get("id")+"/"+expected).toAbsolutePath().normalize();
  if(!expected.equals(file.get("path"))||!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)||!path.toRealPath().equals(path)||Files.size(path)!=((Number)file.get("size")).longValue()||!ExecutionInputs.sha(path).equals(file.get("sha256")))throw new ResponseStatusException(HttpStatus.CONFLICT,"来源分片校验失败");
  return path;
 }
 Object decode(Object value){try{return json.readValue(value.toString(),Object.class);}catch(Exception e){throw new IllegalStateException(e);}}
 String encode(Object value){try{return json.writeValueAsString(value);}catch(Exception e){throw new IllegalStateException(e);}}
 public Map<String,Object> upload(MultipartFile upload)throws Exception{
  if(upload.isEmpty())throw bad("请选择非空 CSV 文件");
  try(var input=upload.getInputStream()){return register(upload.getOriginalFilename(),input,false);}
 }
 public Map<String,Object> uploadParts(List<MultipartFile> files)throws Exception{
  String identity=id();Files.createDirectories(ROOT.resolve("sources"));Path directory=ROOT.resolve("sources/"+identity);
  var merged=CsvParts.merge(files,directory);
  Map<String,Object> data;
  try(var input=Files.newInputStream(merged.csv())){data=register("merged-"+identity+".csv",input,false,identity);}
  if(((Number)data.get("row_count")).longValue()!=merged.rows()||!data.get("columns").equals(merged.columns()))throw new IllegalStateException("合并数据核验不一致");
  var manifest=Map.of("version",1,"format","CSV_PARTS","order","REQUEST","files",merged.files(),"columns",merged.columns(),"rowCount",merged.rows(),"canonicalSha256",data.get("sha256"));
  db.update("update datasets set source_manifest_json=? where id=?",encode(manifest),identity);
  // Only this generated scratch file is removed; exact source uploads remain immutable.
  Files.delete(merged.csv());
  return one(identity);
 }
 Map<String,Object> register(String name,InputStream source,boolean allowEmpty)throws Exception{return register(name,source,allowEmpty,id());}
 Map<String,Object> registerControlled(String name,Path path,boolean allowEmpty,Map<String,Object> provenance,String expectedSha)throws Exception{
  if(!ControlledProfile.REGISTRATION.tryAcquire())throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,"已有受控文件正在登记，请稍后重试");
  boolean deferred=false;
  try{
   if(expectedSha!=null){
    var prior=db.queryForList("select id from datasets where JSON_UNQUOTE(JSON_EXTRACT(source_manifest_json,'$.format'))='CONTROLLED_CSV' and JSON_UNQUOTE(JSON_EXTRACT(source_manifest_json,'$.kind'))='PRESET' and JSON_UNQUOTE(JSON_EXTRACT(source_manifest_json,'$.sourceId'))=? and sha256=? order by created_at,id limit 1",provenance.get("sourceId"),expectedSha);
    if(!prior.isEmpty()){var data=one(prior.get(0).get("id").toString());if(!Objects.equals(((Map<?,?>)data.get("source")).get("hive"),provenance.get("hive")))throw new ResponseStatusException(HttpStatus.CONFLICT,"同一别名与原文的来源凭据已改变，请配置新别名");return data;}
   }
   ControlledProfile.admit(ROOT);
   if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)||!path.toRealPath().equals(path.toAbsolutePath().normalize()))throw bad("受控输入必须是无链接的普通文件");
   Map<String,Object> result;
   try(var stream=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS)){result=register(name,stream,allowEmpty,id(),provenance,expectedSha);}
   if(org.springframework.transaction.support.TransactionSynchronizationManager.isSynchronizationActive()){
    org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(new org.springframework.transaction.support.TransactionSynchronization(){@Override public void afterCompletion(int status){ControlledProfile.REGISTRATION.release();}});deferred=true;
   }
   return result;
  }finally{if(!deferred)ControlledProfile.REGISTRATION.release();}
 }
 Map<String,Object> register(String name,InputStream source,boolean allowEmpty,String identity)throws Exception{return register(name,source,allowEmpty,identity,null,null);}
 Map<String,Object> register(String name,InputStream source,boolean allowEmpty,String identity,Map<String,Object> provenance,String expectedSha)throws Exception{
  final boolean controlled=provenance!=null;final long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(120);

  if(name==null||!name.toLowerCase(Locale.ROOT).endsWith(".csv"))throw bad("请选择 CSV 文件");
  name=name.replace('\\','/');name=name.substring(name.lastIndexOf('/')+1);
  if(name.length()>200)throw bad("文件名过长");
  Files.createDirectories(ROOT.resolve("raw"));
  String id=identity;Path temp=ROOT.resolve("raw/"+id+".upload"), target=ROOT.resolve("raw/"+id+".csv");
  try{
   try(var input=source;var out=Files.newOutputStream(temp,StandardOpenOption.CREATE_NEW)){
    byte[] buffer=new byte[8192];long size=0;int n;
    while((n=input.read(buffer))!=-1){size+=n;if(controlled)ControlledProfile.checkpoint(deadline);if(size>(controlled?ControlledProfile.MAX_BYTES:20L*1024*1024))throw bad(controlled?"受控文件不能超过64MiB":"文件不能超过 20 MiB");out.write(buffer,0,n);}
   }
   var decoder=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
   var preview=new ArrayList<List<String>>();List<String> headers;long count=0;
   try(var reader=new PushbackReader(new InputStreamReader(Files.newInputStream(temp),decoder),1)){
    int first=reader.read();if(first!=-1&&first!=0xfeff)reader.unread(first);
    // Header is read as a record so duplicate and empty names cannot be silently normalized.
    try(var csv=CSVFormat.RFC4180.builder().setIgnoreEmptyLines(true).get().parse(new CsvLimitsReader(reader,controlled?deadline:Long.MAX_VALUE))){
     var it=csv.iterator();if(!it.hasNext())throw bad("CSV 缺少表头");headers=it.next().toList();
     if(headers.isEmpty()||headers.size()>100||new HashSet<>(headers).size()!=headers.size()||headers.stream().anyMatch(h->!h.matches("[A-Za-z][A-Za-z0-9_]{0,63}")))throw bad("字段名须唯一，使用英文字母开头的字母数字下划线，最多 64 字符/100 列");
     while(it.hasNext()){
      var row=it.next();if(row.size()!=headers.size())throw bad("第 "+(count+2)+" 条 CSV 记录列数不匹配");
      for(String value:row)if(value.indexOf(0)>=0||value.length()>65536)throw bad("单元格包含 NUL 或长度超限");
      count++;if(controlled){ControlledProfile.checkpoint(deadline);if(count>ControlledProfile.MAX_ROWS)throw bad("受控文件不能超过100万行");}if(preview.size()<10)preview.add(row.stream().map(CsvLimitsReader::preview).toList());
     }
    }
   }catch(ResponseStatusException e){throw e;}catch(Exception e){throw bad("CSV 必须为 UTF-8 且符合 RFC4180 格式");}
   if(count==0&&!allowEmpty)throw bad("CSV 至少需要一条数据记录");
   var digest=MessageDigest.getInstance("SHA-256");try(var input=Files.newInputStream(temp)){byte[] b=new byte[8192];int n;while((n=input.read(b))!=-1){if(controlled)ControlledProfile.checkpoint(deadline);digest.update(b,0,n);}}
   String sha=HexFormat.of().formatHex(digest.digest());
   if(expectedSha!=null&&!expectedSha.equals(sha))throw bad("受控文件指纹与白名单不一致，未发布数据集");
   if(controlled&&provenance.get("hive") instanceof Map<?,?> hive){if(!Objects.equals(hive.get("canonicalSha256"),sha)||((Number)hive.get("rows")).longValue()!=count||!Objects.equals(hive.get("columns"),headers))throw bad("Hive Catalog 导出凭据与规范快照不一致");}
   Files.move(temp,target,StandardCopyOption.ATOMIC_MOVE);
   target.toFile().setReadOnly();
   if(controlled){
    var manifest=new LinkedHashMap<String,Object>(provenance);manifest.put("version",1);manifest.put("format","CONTROLLED_CSV");manifest.put("profile","CONTROLLED_64M_V1");manifest.put("canonicalSha256",sha);manifest.put("rowCount",count);manifest.put("columns",headers);manifest.put("files",List.of(Map.of("path","canonical.csv","name",name,"size",Files.size(target),"sha256",sha,"rows",count)));
    db.update("insert into datasets(id,name,file_path,sha256,size_bytes,row_count,columns_json,preview_json,source_manifest_json) values(?,?,?,?,?,?,?,?,?)",id,name,target.toString(),sha,Files.size(target),count,encode(headers),encode(preview),encode(manifest));
   }else db.update("insert into datasets(id,name,file_path,sha256,size_bytes,row_count,columns_json,preview_json) values(?,?,?,?,?,?,?,?)",id,name,target.toString(),sha,Files.size(target),count,encode(headers),encode(preview));
   return one(id);
  }finally{Files.deleteIfExists(temp);}
 }
}
