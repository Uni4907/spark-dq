package cn.sparkdq;

import jakarta.annotation.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Shares the serial detection queue. Credentials remain only in the private connection factory. */
@Service @org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization
public class MysqlImports {
 record Request(String sourceId,List<String> columns) {}
 final Datasets ds;final Tasks tasks;final MysqlSources sources;final TransactionTemplate tx;
 final ConcurrentHashMap<String,Job> jobs=new ConcurrentHashMap<>();volatile boolean closing;
 final class Job implements Runnable {
  final String id;final MysqlSources.Source source;final List<String> selection;final MysqlSnapshot.Control control=new MysqlSnapshot.Control();
  final CountDownLatch stopped=new CountDownLatch(1);
  Job(String id,MysqlSources.Source source,List<String> selection){this.id=id;this.source=source;this.selection=List.copyOf(selection);}
  public void run(){execute(this);}
 }
 public MysqlImports(Datasets ds,Tasks tasks,MysqlSources sources,PlatformTransactionManager manager){this.ds=ds;this.tasks=tasks;this.sources=sources;tx=new TransactionTemplate(manager);}
 @PostConstruct void recover(){
  // The previous backend process no longer owns JDBC connections. Retain all interrupted files as evidence.
  ds.db.update("update dataset_imports set status='CANCELLED',error='服务重启，连接已退出；保留中断证据',finished_at=CURRENT_TIMESTAMP(3) where source_kind='MYSQL_SNAPSHOT' and status='CANCELLING'");
  ds.db.update("update dataset_imports set status='FAILED',error='服务重启中断业务快照；重新导入将取得新快照',finished_at=CURRENT_TIMESTAMP(3) where source_kind='MYSQL_SNAPSHOT' and status in ('QUEUED','RUNNING')");
 }
 @PreDestroy void stop(){
  synchronized(tasks){closing=true;for(var job:jobs.values()){
   job.control.cancel();if(tasks.pool.remove(job)){ds.db.update("update dataset_imports set status='FAILED',error='服务停止中断排队业务快照',finished_at=CURRENT_TIMESTAMP(3) where id=? and status='QUEUED'",job.id);jobs.remove(job.id);job.stopped.countDown();}
  }}
  for(var job:jobs.values())try{job.stopped.await(15,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();break;}
 }
 static String requestJson(Datasets ds,Request request){return ds.encode(Map.of("sourceId",request.sourceId(),"columns",request.columns()));}
 public Map<String,Object> submit(Request request,String key)throws Exception {
  NativeImports.key(key);if(request==null||request.sourceId()==null||request.columns()==null)throw Datasets.bad("需要业务源与字段选择");
  var source=sources.source(request.sourceId());var selected=MysqlSnapshot.select(source.table,request.columns());
  String definition=requestJson(ds,new Request(source.id,selected.stream().map(MysqlSnapshot.Column::source).toList()));
  synchronized(tasks){
   var existing=ds.db.queryForList("select * from dataset_imports where request_key=?",key);
   if(!existing.isEmpty()){
    var row=existing.get(0);if(!"MYSQL_SNAPSHOT".equals(row.get("source_kind"))||!Objects.equals(ds.decode(row.get("source_request_json")),ds.decode(definition)))throw new ResponseStatusException(HttpStatus.CONFLICT,"此导入标识已用于不同请求");
    return row;
   }
   if(closing||tasks.pool.isShutdown())throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"服务停止中");
   if(tasks.pool.getQueue().remainingCapacity()==0)throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,"任务队列已满");
   Path base=Datasets.ROOT.resolve("mysql-imports");Files.createDirectories(base);long retained=0;int entries=0;
   try(var walk=Files.walk(base)){for(var it=walk.iterator();it.hasNext();){Path p=it.next();if(++entries>10000||Files.isSymbolicLink(p))throw new ResponseStatusException(HttpStatus.INSUFFICIENT_STORAGE,"业务快照保留预算达到上限");if(Files.isRegularFile(p))retained+=Files.size(p);}}
   if(Files.getFileStore(base).getUsableSpace()<1024L*1024*1024||retained+(jobs.size()+1L)*64*1024*1024>1024L*1024*1024)throw new ResponseStatusException(HttpStatus.INSUFFICIENT_STORAGE,"业务快照空间不足");
   String id=Datasets.id();Files.createDirectory(base.resolve(id));
   ds.db.update("insert into dataset_imports(id,request_key,name,source_kind,source_request_json,status) values(?,?,?,'MYSQL_SNAPSHOT',?,'QUEUED')",id,key,"mysql-"+source.id+".csv",definition);
   var job=new Job(id,source,request.columns());jobs.put(id,job);
   try{tasks.pool.execute(job);}catch(RejectedExecutionException error){jobs.remove(id);ds.db.update("update dataset_imports set status='FAILED',error='调度器停止',finished_at=CURRENT_TIMESTAMP(3) where id=?",id);job.stopped.countDown();}
   return detail(id);
  }
 }
 Map<String,Object> locked(String id){var rows=ds.db.queryForList("select * from dataset_imports where id=? and source_kind='MYSQL_SNAPSHOT' for update",id);if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"业务快照导入不存在");return rows.get(0);}
 public Map<String,Object> detail(String id){var rows=ds.db.queryForList("select * from dataset_imports where id=? and source_kind='MYSQL_SNAPSHOT'",id);if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"业务快照导入不存在");return rows.get(0);}
 public Map<String,Object> cancel(String id){
  synchronized(tasks){
   tx.executeWithoutResult(s->{String state=locked(id).get("status").toString();
    if("QUEUED".equals(state))ds.db.update("update dataset_imports set status='CANCELLED',error='用户取消排队业务快照',finished_at=CURRENT_TIMESTAMP(3),duration_ms=0 where id=?",id);
    else if("RUNNING".equals(state))ds.db.update("update dataset_imports set status='CANCELLING',error='正在关闭业务连接' where id=?",id);
    else if(!Set.of("CANCELLING","CANCELLED").contains(state))throw new ResponseStatusException(HttpStatus.CONFLICT,"此业务快照导入已结束");
   });
   var job=jobs.get(id);if(job!=null){job.control.cancel();if(tasks.pool.remove(job)){jobs.remove(id);job.stopped.countDown();}}
   return detail(id);
  }
 }
 void execute(Job job){
  long start=System.nanoTime();String failure=null;
  try {
   if(ds.db.update("update dataset_imports set status='RUNNING',started_at=CURRENT_TIMESTAMP(3) where id=? and status='QUEUED'",job.id)!=1)return;
   if(closing||tasks.pool.isShutdown())job.control.cancel();
   Path snapshot=Datasets.ROOT.resolve("mysql-imports/"+job.id+"/snapshot");
   var proof=MysqlSnapshot.export(job.source::open,job.source.table,job.selection,job.source.limits,job.control,snapshot);
   String manifestSha=ExecutionInputs.sha(snapshot.resolve("manifest.json"));
   synchronized(tasks){tx.executeWithoutResult(s->{
    if(closing||tasks.pool.isShutdown()||!"RUNNING".equals(locked(job.id).get("status")))throw new CancellationException();
    try(var input=Files.newInputStream(snapshot.resolve("canonical.csv"))){
     if(!ExecutionInputs.sha(snapshot.resolve("canonical.csv")).equals(proof.sha256()))throw new IllegalStateException();
     var data=ds.register("mysql-"+job.source.id+".csv",input,false,job.id);
     if(!proof.sha256().equals(data.get("sha256"))||proof.rows()!=((Number)data.get("row_count")).longValue()||!proof.columns().equals(data.get("columns")))throw new IllegalStateException();
     var manifest=new LinkedHashMap<String,Object>();manifest.put("version",1);manifest.put("format","MYSQL_SNAPSHOT");manifest.put("order","UNSPECIFIED");manifest.put("importId",job.id);manifest.put("sourceId",job.source.id);manifest.put("schema",job.source.table.schema());manifest.put("table",job.source.table.name());
     manifest.put("mapping",MysqlSnapshot.select(job.source.table,job.selection));manifest.put("types",proof.types());manifest.put("columns",proof.columns());manifest.put("rowCount",proof.rows());manifest.put("canonicalSha256",proof.sha256());manifest.put("snapshotManifestSha256",manifestSha);
     manifest.put("files",List.of(Map.of("path","canonical.csv","name","mysql-"+job.source.id+".csv","size",proof.bytes(),"sha256",proof.sha256(),"rows",proof.rows())));
     ds.db.update("update datasets set source_manifest_json=? where id=?",ds.encode(manifest),job.id);
     ds.db.update("update dataset_imports set status='SUCCEEDED',dataset_id=?,source_sha256=?,source_bytes=?,manifest_sha256=?,duration_ms=?,finished_at=CURRENT_TIMESTAMP(3) where id=?",job.id,proof.sha256(),proof.bytes(),manifestSha,(System.nanoTime()-start)/1000000,job.id);
    }catch(Exception error){throw new IllegalStateException("MYSQL_SNAPSHOT_REGISTRATION_FAILED");}
   });}
  }catch(MysqlSnapshot.Failure error){failure=error.code;}
   catch(CancellationException error){failure="CANCELLED";}
   catch(Exception error){failure="MYSQL_SNAPSHOT_IMPORT_FAILED";}
  finally {
   try {
    if(failure!=null)ds.db.update("update dataset_imports set status='FAILED',error=?,duration_ms=?,finished_at=CURRENT_TIMESTAMP(3) where id=? and status='RUNNING'",failure,(System.nanoTime()-start)/1000000,job.id);
    ds.db.update("update dataset_imports set status='CANCELLED',error='业务连接已关闭，未发布数据集',duration_ms=?,finished_at=CURRENT_TIMESTAMP(3) where id=? and status='CANCELLING'",(System.nanoTime()-start)/1000000,job.id);
   }catch(Exception error){Tasks.LOG.warn("MySQL import state update deferred for {}",job.id);}
   finally {jobs.remove(job.id);job.stopped.countDown();}
  }
 }
 public Pages.Page page(String page,String pageSize){var b=Pages.bounds(page,pageSize);return tx.execute(s->{long count=ds.db.queryForObject("select count(*) from dataset_imports where source_kind='MYSQL_SNAPSHOT'",Long.class);return new Pages.Page(ds.db.queryForList("select * from dataset_imports where source_kind='MYSQL_SNAPSHOT' order by created_at desc,id desc limit ? offset ?",b.size(),b.offset()),count,b.page(),b.size(),count/b.size()+(count%b.size()==0?0:1));});}
}
