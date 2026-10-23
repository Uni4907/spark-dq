package cn.sparkdq;

import jakarta.annotation.*;
import java.nio.file.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Native conversions share the detection queue and process ownership, but publish atomically. */
@Service @org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization
public class NativeImports {
 final Datasets ds;final Tasks tasks;final TransactionTemplate tx;
 final ConcurrentHashMap<String,ImportJob> jobs=new ConcurrentHashMap<>();
 final ScheduledExecutorService cleanup=Executors.newSingleThreadScheduledExecutor(action->{var t=new Thread(action,"sparkdq-import-cleanup");t.setDaemon(true);return t;});
 volatile boolean closing;
 final class ImportJob implements Runnable {
  final String id;final Path root;final Tasks.Job owner;final long size;volatile String failure;
  ImportJob(String id,String sha,long size){this.id=id;this.size=size;root=directory(id);owner=tasks.new Job(id,root.resolve("source.parquet").toString(),List.of(),sha,null,"csv-v2");}
  public void run(){execute(this);}
 }
 public NativeImports(Datasets ds,Tasks tasks,PlatformTransactionManager manager){this.ds=ds;this.tasks=tasks;tx=new TransactionTemplate(manager);cleanup.scheduleWithFixedDelay(this::reap,1,1,TimeUnit.SECONDS);}
 Path directory(String id){return Datasets.ROOT.resolve("imports").resolve(id);}
 static String key(String key){try{if(key==null||!UUID.fromString(key).toString().equals(key))throw new IllegalArgumentException();return key;}catch(IllegalArgumentException e){throw Datasets.bad("需要 UUID 格式的 X-Import-Key");}}
 static String filename(String name){if(name==null)throw Datasets.bad("缺少 Parquet 文件名");name=name.replace('\\','/');name=name.substring(name.lastIndexOf('/')+1);if(name.length()>200||!name.toLowerCase(Locale.ROOT).endsWith(".parquet")||name.chars().anyMatch(Character::isISOControl))throw Datasets.bad("请选择名称不超过200字符的 Parquet 文件");return name;}
 @PostConstruct void recover(){
  ds.db.update("update dataset_imports set status='CANCELLED',error='服务重启完成取消清理',finished_at=CURRENT_TIMESTAMP(3),duration_ms=coalesce(TIMESTAMPDIFF(MICROSECOND,started_at,CURRENT_TIMESTAMP(3))/1000,0) where source_kind='PARQUET_NATIVE' and status='CANCELLING'");
  ds.db.update("update dataset_imports set status='FAILED',error='服务重启中断导入，可从保留的原文件重试',finished_at=CURRENT_TIMESTAMP(3),duration_ms=coalesce(TIMESTAMPDIFF(MICROSECOND,started_at,CURRENT_TIMESTAMP(3))/1000,0) where source_kind='PARQUET_NATIVE' and status in ('QUEUED','RUNNING')");
 }
 @PreDestroy void stop(){
  synchronized(tasks){closing=true;for(var job:jobs.values()){
   job.owner.cancelled=true;tasks.signal(job.owner);
   if(tasks.pool.remove(job)){ds.db.update("update dataset_imports set status='FAILED',error='服务停止中断排队导入，可重试',duration_ms=0,finished_at=CURRENT_TIMESTAMP(3) where id=? and status='QUEUED'",job.id);jobs.remove(job.id,job);}
  }}
  cleanup.shutdownNow();for(var job:jobs.values())tasks.terminateAndAwait(job.owner);
 }
 public Map<String,Object> upload(MultipartFile file,String requestKey)throws Exception {
  String name=filename(file.getOriginalFilename());key(requestKey);
  try(var stream=file.getInputStream()){return submit(name,stream,requestKey,null);}
 }
 Map<String,Object> submit(String name,InputStream stream,String requestKey,String parent)throws Exception {
  Path base=Datasets.ROOT.resolve("imports");Files.createDirectories(base);
  Path scratch=Files.createTempFile(base,"incoming-",".upload");
  try {
   long size=0;try(var out=Files.newOutputStream(scratch)){byte[] b=new byte[65536];int n;while((n=stream.read(b))!=-1){size+=n;if(size>NativeProof.LIMIT)throw Datasets.bad("原文件不能超过20 MiB");out.write(b,0,n);}}
   if(size<12)throw Datasets.bad("Parquet 文件过短");String sha=ExecutionInputs.sha(scratch);
   synchronized(tasks) {
    var existing=ds.db.queryForList("select * from dataset_imports where request_key=?",requestKey);
    if(!existing.isEmpty()) {
     var row=existing.get(0);
     if(!"PARQUET_NATIVE".equals(row.get("source_kind"))||!name.equals(row.get("name"))||!sha.equals(row.get("source_sha256"))||((Number)row.get("source_bytes")).longValue()!=size||!Objects.equals(parent,row.get("parent_import_id")))throw new ResponseStatusException(HttpStatus.CONFLICT,"此导入标识已用于不同文件或重试来源");
     return row;
    }
    if(closing||tasks.pool.isShutdown())throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"服务停止中");
    if(tasks.pool.getQueue().remainingCapacity()==0)throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,"任务队列已满");
    // Conservative admission bound for retained originals, worker and canonical copies.
    long retained=0;try(var paths=Files.walk(base)){var it=paths.iterator();int count=0;while(it.hasNext()){Path p=it.next();if(++count>10000)throw new ResponseStatusException(HttpStatus.INSUFFICIENT_STORAGE,"导入文件数量达到保留预算");if(Files.isSymbolicLink(p))throw new IllegalStateException("导入目录包含链接");if(Files.isRegularFile(p))retained+=Files.size(p);}}
    if(Files.getFileStore(base).getUsableSpace()<1024L*1024*1024||retained+(jobs.size()+1L)*64*1024*1024>1024L*1024*1024)throw new ResponseStatusException(HttpStatus.INSUFFICIENT_STORAGE,"导入保留空间不足");
    String id=Datasets.id();Path root=directory(id);Files.createDirectory(root);Path source=root.resolve("source.parquet");Files.move(scratch,source,StandardCopyOption.ATOMIC_MOVE);if(!source.toFile().setReadOnly())throw new IOException("无法冻结原文件");
    ds.db.update("insert into dataset_imports(id,request_key,name,source_sha256,source_bytes,status,parent_import_id) values(?,?,?,?,?,'QUEUED',?)",id,requestKey,name,sha,size,parent);
    var job=new ImportJob(id,sha,size);jobs.put(id,job);
    try{tasks.pool.execute(job);}catch(RejectedExecutionException e){jobs.remove(id,job);ds.db.update("update dataset_imports set status='FAILED',error='调度器停止，导入未执行',duration_ms=0,finished_at=CURRENT_TIMESTAMP(3) where id=?",id);}
    return detail(id);
   }
  } finally {Files.deleteIfExists(scratch);}
 }
 public Map<String,Object> retry(String id,String requestKey)throws Exception {
  key(requestKey);var row=detail(id);
  if(!Set.of("FAILED","CANCELLED").contains(row.get("status")))throw new ResponseStatusException(HttpStatus.CONFLICT,"仅失败或取消的导入可重试");
  Path source=NativeProof.file(directory(id).resolve("source.parquet"),12,NativeProof.LIMIT);
  if(Files.size(source)!=((Number)row.get("source_bytes")).longValue()||!ExecutionInputs.sha(source).equals(row.get("source_sha256")))throw new ResponseStatusException(HttpStatus.CONFLICT,"保留原文件指纹不匹配");
  try(var stream=Files.newInputStream(source)){return submit(row.get("name").toString(),stream,requestKey,id);}
 }
 String locked(String id){var rows=ds.db.queryForList("select status from dataset_imports where id=? and source_kind='PARQUET_NATIVE' for update",id);if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"导入不存在");return rows.get(0).get("status").toString();}
 public Map<String,Object> cancel(String id){
  synchronized(tasks) {
   tx.executeWithoutResult(s->{String state=locked(id);
    if("QUEUED".equals(state))ds.db.update("update dataset_imports set status='CANCELLED',error='用户取消排队导入',duration_ms=0,finished_at=CURRENT_TIMESTAMP(3) where id=?",id);
    else if("RUNNING".equals(state))ds.db.update("update dataset_imports set status='CANCELLING',error='正在终止导入进程' where id=?",id);
    else if(!Set.of("CANCELLING","CANCELLED").contains(state))throw new ResponseStatusException(HttpStatus.CONFLICT,"已成功或失败的导入不能取消");
   });
   var job=jobs.get(id);if(job!=null){job.owner.cancelled=true;tasks.signal(job.owner);if(tasks.pool.remove(job))jobs.remove(id,job);}
   return detail(id);
  }
 }
 void finish(ImportJob job){
  long elapsed=(System.nanoTime()-job.owner.startedNanos)/1000000;
  // Terminal failures are published only after the owned process has actually stopped.
  if(job.failure!=null)ds.db.update("update dataset_imports set status='FAILED',error=?,finished_at=CURRENT_TIMESTAMP(3),duration_ms=? where id=? and status='RUNNING'",job.failure,elapsed,job.id);
  ds.db.update("update dataset_imports set status='CANCELLED',error='导入进程及子进程已停止',finished_at=CURRENT_TIMESTAMP(3),duration_ms=? where id=? and status='CANCELLING'",elapsed,job.id);jobs.remove(job.id,job);
 }
 void reap(){for(var job:jobs.values())if(job.owner.runnerFinished)try{tasks.signal(job.owner);if(tasks.fullyStopped(job.owner))finish(job);}catch(Exception e){Tasks.LOG.warn("Import cleanup will retry for {}",job.id,e);}}
 void execute(ImportJob job){
  long start=System.nanoTime();job.owner.startedNanos=start;
  try {
   if(ds.db.update("update dataset_imports set status='RUNNING',started_at=CURRENT_TIMESTAMP(3) where id=? and status='QUEUED'",job.id)!=1)return;
   tasks.checkpoint(job.owner);Path source=NativeProof.file(job.root.resolve("source.parquet"),12,NativeProof.LIMIT);
   if(!ExecutionInputs.sha(source).equals(job.owner.expectedSha))throw new IllegalStateException("原文件指纹不匹配");
   Path request=job.root.resolve("request.json");ds.json.writeValue(request.toFile(),Map.of("input",source.toString(),"output",job.root.resolve("prepared").toString(),"sourceSha256",job.owner.expectedSha));
   tasks.runWorker(job.owner,"cn.sparkdq.engine.ParquetImport",request,job.root.resolve("engine.log"),start);
   tasks.checkpoint(job.owner);var proof=NativeProof.verify(job.root,job.owner.expectedSha,job.size,ds.json);
   tx.executeWithoutResult(s->{
    if(!"RUNNING".equals(locked(job.id)))throw new CancellationException("导入已停止，未登记数据集");
    tasksCheckpoint(job);
    try(var input=Files.newInputStream(job.root.resolve("prepared/canonical.csv"))) {
     var data=ds.register("imported-"+job.id+".csv",input,false,job.id);
     if(!proof.canonicalSha().equals(data.get("sha256"))||proof.rows()!=((Number)data.get("row_count")).longValue()||!proof.columns().equals(data.get("columns")))throw new IllegalStateException("导入登记校验不一致");
     var row=detail(job.id);var manifest=new LinkedHashMap<String,Object>();
     manifest.put("version",1);manifest.put("format","PARQUET_NATIVE");manifest.put("order","SINGLE");manifest.put("importId",job.id);manifest.put("columns",proof.columns());manifest.put("rowCount",proof.rows());manifest.put("canonicalSha256",proof.canonicalSha());manifest.put("nativeManifestSha256",proof.manifestSha());manifest.put("nativeColumns",proof.manifest().get("columns"));manifest.put("conversion",proof.manifest().get("conversion"));
     manifest.put("files",List.of(Map.of("path","source.parquet","name",row.get("name"),"size",job.size,"sha256",job.owner.expectedSha,"rows",proof.rows())));
     ds.db.update("update datasets set source_manifest_json=? where id=?",ds.encode(manifest),job.id);
     ds.db.update("update dataset_imports set status='SUCCEEDED',dataset_id=?,manifest_sha256=?,duration_ms=?,finished_at=CURRENT_TIMESTAMP(3) where id=?",job.id,proof.manifestSha(),(System.nanoTime()-start)/1000000,job.id);
    }catch(Exception e){throw new IllegalStateException("导入登记失败: "+e.getMessage(),e);}
   });
  }catch(Exception e){
   String reason=e instanceof InterruptedException?"服务停止中断导入，可从保留原文件重试":e.getMessage()==null?e.getClass().getSimpleName():e.getMessage();
   job.failure=reason.substring(0,Math.min(5000,reason.length()));
   if(e instanceof InterruptedException)Thread.currentThread().interrupt();
  }finally{try{if(tasks.terminateAndAwait(job.owner))finish(job);}catch(Exception e){Tasks.LOG.warn("Import cleanup will retry for {}",job.id,e);}finally{job.owner.runnerFinished=true;}}
 }
 void tasksCheckpoint(ImportJob job){try{tasks.checkpoint(job.owner);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new CancellationException("服务停止中");}}
 public Map<String,Object> detail(String id){var rows=ds.db.queryForList("select * from dataset_imports where id=? and source_kind='PARQUET_NATIVE'",id);if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"导入不存在");return rows.get(0);}
 public Pages.Page page(String page,String pageSize){var b=Pages.bounds(page,pageSize);return tx.execute(s->{long total=ds.db.queryForObject("select count(*) from dataset_imports where source_kind='PARQUET_NATIVE'",Long.class);var items=ds.db.queryForList("select * from dataset_imports where source_kind='PARQUET_NATIVE' order by created_at desc,id desc limit ? offset ?",b.size(),b.offset());return new Pages.Page(items,total,b.page(),b.size(),total/b.size()+(total%b.size()==0?0:1));});}
}
