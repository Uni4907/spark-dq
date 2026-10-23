package cn.sparkdq;

import com.fasterxml.jackson.databind.*;
import jakarta.annotation.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.nio.file.*;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.*;

@Service
@org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization
public class Tasks {
 static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(Tasks.class);
 final Datasets ds;final TransactionTemplate tx;final ClusterExecution cluster;
 final Set<String> recoveryPending=ConcurrentHashMap.newKeySet();
 final ThreadPoolExecutor pool=new ThreadPoolExecutor(1,1,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(8));
 // Lifecycle state belongs to a task, never to a shared "current process" slot.
 final ConcurrentHashMap<String,Job> jobs=new ConcurrentHashMap<>();
 final ScheduledExecutorService cleanup=Executors.newSingleThreadScheduledExecutor(action->{var thread=new Thread(action,"sparkdq-task-cleanup");thread.setDaemon(true);return thread;});
 final class Job implements Runnable {
  final String id,input,expectedSha,semanticsVersion;final List<Map<String,Object>> rules;final GovernanceRequest policy;final ExecutionInputs.Frozen executionInput;
  final Set<ProcessHandle> descendants=ConcurrentHashMap.newKeySet();
  volatile boolean controlled,cancelled,runnerFinished;volatile String failureReason;volatile Process process;volatile long startedNanos;
  Job(String id,String input,List<Map<String,Object>> rules,String expectedSha,GovernanceRequest policy,String semanticsVersion){this(id,input,rules,expectedSha,policy,semanticsVersion,null);}
  Job(String id,String input,List<Map<String,Object>> rules,String expectedSha,GovernanceRequest policy,String semanticsVersion,ExecutionInputs.Frozen executionInput){this.executionInput=executionInput;this.id=id;this.input=input;this.rules=rules;this.expectedSha=expectedSha;this.policy=policy;this.semanticsVersion=semanticsVersion;}
  public void run(){execute(this);}
 }
 public Tasks(Datasets ds,PlatformTransactionManager manager){this.ds=ds;tx=new TransactionTemplate(manager);cluster=new ClusterExecution(ClusterExecution.settings(System.getenv()),ds.json);ClusterExecution.budget(System.getenv());if(cluster.profile.remote()&&ProcessHandle.current().pid()!=1)throw new IllegalStateException("Standalone client requires the backend JVM as container PID 1");cleanup.scheduleWithFixedDelay(this::reap,1,1,TimeUnit.SECONDS);}
 @PostConstruct void recover(){
  var interrupted=ds.db.queryForList("select id,status from tasks where status in ('QUEUED','RUNNING','CANCELLING')");
  if(cluster.profile.remote()||interrupted.stream().anyMatch(row->cluster.hasIntent(taskDirectory(row.get("id").toString()))||cluster.foreignRemote(taskDirectory(row.get("id").toString()),row.get("id").toString()))){
   for(var row:interrupted){String id=row.get("id").toString();recoveryPending.add(id);ds.db.update("update tasks set error='服务重启中断，等待集群资源清理核对' where id=? and status in ('QUEUED','RUNNING','CANCELLING')",id);}
   return;
  }

  ds.db.update("update tasks set status='CANCELLED',error='服务重启时完成取消中断清理，任务已取消',finished_at=CURRENT_TIMESTAMP(3),duration_ms=CASE WHEN started_at IS NULL THEN 0 ELSE TIMESTAMPDIFF(MICROSECOND,started_at,CURRENT_TIMESTAMP(3))/1000 END where status='CANCELLING'");
  ds.db.update("update tasks set status='FAILED',error='服务重启中断任务，请重新提交',finished_at=CURRENT_TIMESTAMP(3),duration_ms=CASE WHEN started_at IS NULL THEN 0 ELSE TIMESTAMPDIFF(MICROSECOND,started_at,CURRENT_TIMESTAMP(3))/1000 END where status in ('QUEUED','RUNNING')");
 }
 @PreDestroy void stop(){
  cleanup.shutdownNow();var queued=pool.shutdownNow();jobs.values().forEach(this::signal);
  for(Runnable item:queued)if(item instanceof Tasks.Job job)jobs.remove(job.id,job);
  boolean interrupted=false;
  try{if(!pool.awaitTermination(15,TimeUnit.SECONDS))jobs.values().forEach(this::signal);}
  catch(InterruptedException e){interrupted=true;jobs.values().forEach(this::signal);}
  finally{if(interrupted)Thread.currentThread().interrupt();}
 }
 String lockedStatus(String id){
  var rows=ds.db.queryForList("select status from tasks where id=? for update",id);
  if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"任务不存在");
  return rows.get(0).get("status").toString();
 }
 public Map<String,Object> cancel(String id){
  String state=tx.execute(s->{
   String current=lockedStatus(id);
   if("QUEUED".equals(current)){
    ds.db.update("update tasks set status='CANCELLED',error='用户取消排队任务，未启动 Spark',duration_ms=0,finished_at=CURRENT_TIMESTAMP(3) where id=?",id);return "CANCELLED";
   }
   if("RUNNING".equals(current)){
    ds.db.update("update tasks set status='CANCELLING',error='用户请求取消，正在终止任务执行',duration_ms=TIMESTAMPDIFF(MICROSECOND,started_at,CURRENT_TIMESTAMP(3))/1000 where id=?",id);return "CANCELLING";
   }
   if(Set.of("CANCELLING","CANCELLED").contains(current))return current;
   throw new ResponseStatusException(HttpStatus.CONFLICT,"已成功或失败的任务不能取消");
  });
  Job job;
  // Enqueue is published under this same monitor by create/rerun/govern. A cancel
  // between its database commit and pool.execute must still remove the new queue entry.
  synchronized(this){
   job=jobs.get(id);
   if(job!=null&&Set.of("CANCELLING","CANCELLED").contains(state)){
    job.cancelled=true;if(pool.remove(job))jobs.remove(id,job);
   }
  }
  if(job!=null)signal(job);
  return detail(id);
 }
 void checkpoint(Job job)throws InterruptedException{
  if(job.cancelled)throw new CancellationException("用户取消任务");
  if(pool.isShutdown()||Thread.currentThread().isInterrupted())throw new InterruptedException("服务停止中断任务");
 }
 void signal(Job job){
  Process process=job.process;if(process==null)return;
  process.descendants().forEach(child->{job.descendants.add(child);child.destroyForcibly();});
  job.descendants.forEach(child->{if(child.isAlive())child.destroyForcibly();});
  if(process.isAlive())process.destroyForcibly();
 }
 boolean fullyStopped(Job job){return (job.process==null||!job.process.isAlive())&&job.descendants.stream().noneMatch(ProcessHandle::isAlive)&&cluster.ready(taskDirectory(job.id),job.id);}
 long terminationWaitMillis(){return 10000;}
 void finishStoppedJob(Job job){
  ds.db.update("update tasks set status='CANCELLED',error='用户取消任务，执行及子进程已停止',duration_ms=?,finished_at=CURRENT_TIMESTAMP(3) where id=? and status='CANCELLING'",(System.nanoTime()-job.startedNanos)/1000000,job.id);
  if(job.failureReason!=null&&cluster.profile.remote())ds.db.update("update tasks set status='FAILED',error=?,duration_ms=?,finished_at=CURRENT_TIMESTAMP(3) where id=? and status='RUNNING'",job.failureReason,(System.nanoTime()-job.startedNanos)/1000000,job.id);
  jobs.remove(job.id,job);cluster.forget(job.id);
 }
 void reap(){
  for(String id:recoveryPending)try{
   if(cluster.ready(taskDirectory(id),id)){
    ds.db.update("update tasks set status=CASE WHEN status='CANCELLING' THEN 'CANCELLED' ELSE 'FAILED' END,error='服务重启中断；集群清理核对已通过，请重新提交',finished_at=CURRENT_TIMESTAMP(3),duration_ms=CASE WHEN started_at IS NULL THEN 0 ELSE TIMESTAMPDIFF(MICROSECOND,started_at,CURRENT_TIMESTAMP(3))/1000 END where id=? and status in ('QUEUED','RUNNING','CANCELLING')",id);
    recoveryPending.remove(id);cluster.forget(id);
   }
  }catch(Exception error){LOG.warn("Interrupted cluster task cleanup will retry for {}",id);}

  for(Job job:jobs.values())if(job.runnerFinished)try{
   signal(job);if(fullyStopped(job))finishStoppedJob(job);
  }catch(Exception e){LOG.warn("Task cleanup will retry for {}",job.id,e);}
 }
 boolean terminateAndAwait(Job job){
  // Do not publish CANCELLED until the child and every observed descendant have exited.
  boolean interrupted=Thread.interrupted();long deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(terminationWaitMillis());
  try{
   do{
    signal(job);
    if(fullyStopped(job))return true;
    try{Thread.sleep(cluster.profile.remote()?250:25);}catch(InterruptedException e){interrupted=true;}
   }while(System.nanoTime()<deadline);
   return false;
  }finally{if(interrupted)Thread.currentThread().interrupt();}
 }
 Path taskDirectory(String id){return Datasets.ROOT.resolve("tasks").resolve(id);}
 Process launchProcess(List<String> command,Path log)throws Exception{var builder=new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());cluster.configure(builder,command);return builder.start();}
 public record Request(String datasetId,List<String> fields,List<String> keys,List<Rules.Spec> rules,String templateId,Integer templateRevision,ExecutionInputs.Plan executionInput){
  public Request(String datasetId,List<String> fields,List<String> keys){this(datasetId,fields,keys,null,null,null,null);}
  public Request(String datasetId,List<String> fields,List<String> keys,List<Rules.Spec> rules,String templateId,Integer templateRevision){this(datasetId,fields,keys,rules,templateId,templateRevision,null);}
 }
 public record RerunRequest(String datasetId,ExecutionInputs.Plan executionInput){public RerunRequest(String datasetId){this(datasetId,null);}}
 @com.fasterxml.jackson.databind.annotation.JsonDeserialize(using=ValueMappingReader.class)
 public record ValueMapping(String from,String to){
  @com.fasterxml.jackson.annotation.JsonAnySetter public void unknown(String name,Object value){throw new IllegalArgumentException("未知值映射参数: "+name);}
 }
 public static class ValueMappingReader extends com.fasterxml.jackson.databind.JsonDeserializer<ValueMapping>{
  @Override public ValueMapping deserialize(com.fasterxml.jackson.core.JsonParser parser,com.fasterxml.jackson.databind.DeserializationContext context)throws java.io.IOException {
   com.fasterxml.jackson.databind.JsonNode node=parser.readValueAsTree();
   if(!node.isObject()||node.size()!=2||!node.path("from").isTextual()||!node.path("to").isTextual())
    return context.reportInputMismatch(ValueMapping.class,"映射须只包含字符串 from 和 to");
   return new ValueMapping(node.path("from").asText(),node.path("to").asText());
  }
 }
 @com.fasterxml.jackson.databind.annotation.JsonDeserialize(using=SortSpecReader.class)
 public record SortSpec(String column,String type,String direction,String nulls){}
 public static class SortSpecReader extends com.fasterxml.jackson.databind.JsonDeserializer<SortSpec>{
  @Override public SortSpec deserialize(com.fasterxml.jackson.core.JsonParser parser,com.fasterxml.jackson.databind.DeserializationContext context)throws java.io.IOException {
   com.fasterxml.jackson.databind.JsonNode node=parser.readValueAsTree();
   if(!node.isObject()||node.size()!=4||!node.path("column").isTextual()||!node.path("type").isTextual()||!node.path("direction").isTextual()||!node.path("nulls").isTextual())
    return context.reportInputMismatch(SortSpec.class,"保留顺序须只包含字符串 column、type、direction、nulls");
   return new SortSpec(node.path("column").asText(),node.path("type").asText(),node.path("direction").asText(),node.path("nulls").asText());
  }
 }
 public record GovernanceRequest(boolean dropMissing,boolean deduplicate,Map<String,Object> fills,Map<String,Object> normalizations,@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY) Map<String,List<ValueMapping>> mappings,@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY) List<SortSpec> deduplicationOrder,Integer policyVersion){
  public GovernanceRequest(boolean dropMissing,boolean deduplicate,Map<String,Object> fills,Map<String,Object> normalizations,Map<String,List<ValueMapping>> mappings,Integer policyVersion){this(dropMissing,deduplicate,fills,normalizations,mappings,null,policyVersion);}
  public GovernanceRequest(boolean dropMissing,boolean deduplicate,Map<String,Object> fills,Map<String,Object> normalizations,Integer policyVersion){this(dropMissing,deduplicate,fills,normalizations,null,policyVersion);}
  public GovernanceRequest(boolean dropMissing,boolean deduplicate){this(dropMissing,deduplicate,null,null,null);}
  @com.fasterxml.jackson.annotation.JsonAnySetter public void unknown(String name,Object value){throw new IllegalArgumentException("未知治理参数: "+name);}

 }
 public synchronized Map<String,Object> rerun(String parentId,RerunRequest request){
  var parent=detail(parentId);
  if(!Set.of("SUCCEEDED","FAILED","CANCELLED").contains(parent.get("status")))throw new ResponseStatusException(HttpStatus.CONFLICT,"仅已结束任务可复检");
  String datasetId=request==null||request.datasetId()==null?effectiveDataset(parent):request.datasetId();
  var data=ds.one(datasetId);
  return enqueue(data,copyRules(parentId,parent,data),parentId,null,semantics(parent),null,request!=null&&request.executionInput()!=null?request.executionInput():ExecutionInputs.inherited(parent));
 }
 String semantics(Map<String,Object> task){
  return task.get("snapshot") instanceof Map<?,?> snapshot?snapshot.get("semantics_version").toString():"csv-v1";
 }
 String effectiveDataset(Map<String,Object> task){
  if(task.get("governance") instanceof Map<?,?> run&&run.get("output_dataset_id")!=null)return run.get("output_dataset_id").toString();
  return task.get("dataset_id").toString();
 }
 public synchronized Map<String,Object> govern(String parentId,GovernanceRequest policy){
  if(policy==null)throw Datasets.bad("缺少治理策略");
  var parent=detail(parentId);
  if(!"SUCCEEDED".equals(parent.get("status")))throw new ResponseStatusException(HttpStatus.CONFLICT,"治理需要已成功完成的检测任务");
  if(!"csv-v2".equals(semantics(parent)))throw new ResponseStatusException(HttpStatus.CONFLICT,"历史 CSV 口径仅支持复检；请先对数据集新建检测任务，再治理");
  var data=ds.one(effectiveDataset(parent));
  @SuppressWarnings("unchecked") var columns=(List<String>)data.get("columns");
  policy=GovernancePolicies.normalize(policy,columns);
  var rules=copyRules(parentId,parent,data);
  if(policy.dropMissing()&&rules.stream().noneMatch(r->"MISSING".equals(r.get("type"))))throw Datasets.bad("此任务未配置空值规则");
  if(policy.deduplicate()&&rules.stream().noneMatch(r->"DUPLICATE".equals(r.get("type"))))throw Datasets.bad("此任务未配置重复规则");
  return enqueue(data,rules,parentId,policy,"csv-v2",null,ExecutionInputs.inherited(parent));
 }
 List<Map<String,Object>> copyRules(String parentId,Map<String,Object> parent,Map<String,Object> data){
  @SuppressWarnings("unchecked") var columns=(List<String>)data.get("columns");
  var stored=ds.db.queryForList("select rules_json,semantics_version from task_snapshots where task_id=?",parentId);
  List<?> definitions;
  if(stored.isEmpty())definitions=(List<?>)parent.get("rules");
  else{
   if(!Set.of("csv-v1","csv-v2").contains(stored.get(0).get("semantics_version")))throw new ResponseStatusException(HttpStatus.CONFLICT,"不支持此快照的统计口径版本");
   definitions=(List<?>)ds.decode(stored.get(0).get("rules_json"));
  }
  return Rules.withIds(Rules.normalize(Rules.stored(definitions),columns));
 }
 public synchronized Map<String,Object> create(Request request){
  if(request.datasetId()==null)throw Datasets.bad("缺少数据集 ID");
  var data=ds.one(request.datasetId());
  @SuppressWarnings("unchecked") var columns=(List<String>)data.get("columns");
  if(request.templateId()!=null){
   if(request.rules()!=null||request.fields()!=null||request.keys()!=null)throw Datasets.bad("模板与手动规则不能同时提交");
   var template=RuleTemplates.select(ds,request.templateId(),request.templateRevision());
   var rules=Rules.withIds(Rules.normalize(Rules.stored((List<?>)template.get("rules")),columns));
   return enqueue(data,rules,null,null,"csv-v2",template,request.executionInput());
  }
  if(request.templateRevision()!=null)throw Datasets.bad("templateRevision 需要 templateId");
  if(request.rules()!=null){
   if(request.fields()!=null||request.keys()!=null)throw Datasets.bad("完整规则与简写字段不能同时提交");
   return enqueue(data,Rules.withIds(Rules.normalize(request.rules(),columns)),null,null,"csv-v2",null,request.executionInput());
  }
  check(request.fields(),columns,"检测字段");check(request.keys(),columns,"业务键");
  var rules=new ArrayList<Map<String,Object>>();
  for(String field:request.fields())rules.add(Map.of("id",Datasets.id(),"type","MISSING","columns",List.of(field)));
  rules.add(Map.of("id",Datasets.id(),"type","DUPLICATE","columns",request.keys()));
  return enqueue(data,rules,null,null,"csv-v2",null,request.executionInput());
 }
 private Map<String,Object> enqueue(Map<String,Object> data,List<Map<String,Object>> rules,String parentId,GovernanceRequest policy,String semanticsVersion){
  return enqueue(data,rules,parentId,policy,semanticsVersion,null,null);
 }
 private Map<String,Object> enqueue(Map<String,Object> data,List<Map<String,Object>> rules,String parentId,GovernanceRequest policy,String semanticsVersion,Map<String,Object> template,ExecutionInputs.Plan plan){
  if(!recoveryPending.isEmpty()||jobs.values().stream().anyMatch(job->(job.runnerFinished||job.cancelled||job.failureReason!=null)&&cluster.hasIntent(taskDirectory(job.id))))throw new ResponseStatusException(HttpStatus.CONFLICT,"前序任务清理尚未确认，请稍后重试");
  var frozen=ExecutionInputs.freeze(plan,data,semanticsVersion);
  if(pool.isShutdown())throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"服务停止中，稍后重试");
  if(pool.getQueue().remainingCapacity()==0)throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,"任务队列已满，稍后重试");
  String id=Datasets.id(),sha=data.get("sha256").toString();
  tx.executeWithoutResult(s->{
   ds.db.update("insert into tasks(id,dataset_id,status) values(?,?,'QUEUED')",id,data.get("id"));
   ds.db.update("insert into task_snapshots(task_id,parent_task_id,input_sha256,semantics_version,rules_json) values(?,?,?,?,?)",id,parentId,sha,semanticsVersion,ds.encode(rules));
   if(frozen!=null)ds.db.update("update task_snapshots set execution_plan_json=? where task_id=?",ds.encode(frozen),id);
   if(policy!=null)ds.db.update("insert into governance_runs(task_id,source_task_id,policy_json) values(?,?,?)",id,parentId,ds.encode(policy));
   for(var rule:rules){
    ds.db.update("insert into rules(id,task_id,type,columns_json) values(?,?,?,?)",rule.get("id"),id,rule.get("type"),ds.encode(rule.get("columns")));
    ds.db.update("insert into rule_parameters(rule_id,params_json) values(?,?)",rule.get("id"),ds.encode(rule.getOrDefault("params",Map.of())));
   }
   if(template!=null)ds.db.update("insert into task_template_refs(task_id,template_id,revision) values(?,?,?)",id,template.get("template_id"),template.get("revision"));
   else if(parentId!=null)ds.db.update("insert into task_template_refs(task_id,template_id,revision) select ?,template_id,revision from task_template_refs where task_id=?",id,parentId);
  });
  Job job=new Job(id,data.get("file_path").toString(),rules,sha,policy,semanticsVersion,frozen);job.controlled=ControlledProfile.controlled(data);jobs.put(id,job);
  try{pool.execute(job);}
  catch(RejectedExecutionException e){
   jobs.remove(id,job);
   ds.db.update("update tasks set status='FAILED',error='调度器停止，任务未执行',duration_ms=0,finished_at=CURRENT_TIMESTAMP(3) where id=? and status='QUEUED'",id);
   throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"调度器停止，稍后重试");
  }
  return detail(id);
 }
 void check(List<String> selected,List<String> columns,String label){if(selected==null||selected.isEmpty()||selected.size()>columns.size()||new HashSet<>(selected).size()!=selected.size()||!columns.containsAll(selected))throw Datasets.bad(label+"必须非空、无重复且来自数据集字段");}
 void run(String id,String input,List<Map<String,Object>> rules,String expectedSha){run(id,input,rules,expectedSha,null,"csv-v2");}
 void run(String id,String input,List<Map<String,Object>> rules,String expectedSha,GovernanceRequest policy,String semanticsVersion){
  Job job=new Job(id,input,rules,expectedSha,policy,semanticsVersion);jobs.put(id,job);execute(job);
 }
 void execute(Job job){
  String id=job.id,input=job.input,expectedSha=job.expectedSha,semanticsVersion=job.semanticsVersion;
  List<Map<String,Object>> rules=job.rules;GovernanceRequest policy=job.policy;
  long start=System.nanoTime();job.startedNanos=start;Path dir=taskDirectory(id);
  try{
   if(ds.db.update("update tasks set status='RUNNING',started_at=CURRENT_TIMESTAMP(3) where id=? and status='QUEUED'",id)!=1)return;
   checkpoint(job);
   while(!recoveryPending.isEmpty()||jobs.values().stream().anyMatch(other->other!=job&&other.runnerFinished&&cluster.hasIntent(taskDirectory(other.id)))){
    checkpoint(job);if(System.nanoTime()-start>workerBudgetNanos())throw new IllegalStateException("等待前序集群清理超过任务预算");Thread.sleep(250);
   }
   Files.createDirectories(dir);cluster.begin(dir,id);
   var digest=java.security.MessageDigest.getInstance("SHA-256");
   try(var stream=new java.security.DigestInputStream(Files.newInputStream(Path.of(input)),digest)){
    byte[] bytes=new byte[65536];while(stream.read(bytes)!=-1)checkpoint(job);
   }
   if(!HexFormat.of().formatHex(digest.digest()).equals(expectedSha))throw new IllegalStateException("输入文件 SHA-256 与数据集快照不一致，已停止检测");
   checkpoint(job);
   Files.createDirectories(dir);Path request=dir.resolve("request.json"),log=dir.resolve("engine.log");
   var payload=new LinkedHashMap<String,Object>(Map.of("taskId",id,"input",input,"output",dir.toString(),"rules",rules,"inputSha256",expectedSha,"semanticsVersion",semanticsVersion));
   if(policy!=null)payload.put("governance",policy);
   ds.json.writeValue(request.toFile(),payload);
   if(job.executionInput!=null){
    Path prepareRequest=dir.resolve("prepare-request.json");
    ds.json.writeValue(prepareRequest.toFile(),Map.of("input",input,"output",dir.resolve("prepared").toString(),"format",job.executionInput.format(),"partitions",job.executionInput.partitions()));
    runWorker(job,"cn.sparkdq.engine.PreparedInput",prepareRequest,dir.resolve("prepare.log"),start);
    checkpoint(job);
    String manifestSha=ExecutionInputs.verify(dir.resolve("prepared"),job.executionInput,expectedSha,ds.json);
    tx.executeWithoutResult(status->{
     if(!"RUNNING".equals(lockedStatus(id)))throw new CancellationException("任务已停止，未登记执行副本");
     if(ds.db.update("update task_snapshots set input_manifest_sha256=? where task_id=? and input_manifest_sha256 is null",manifestSha,id)!=1)throw new IllegalStateException("执行副本指纹已登记，拒绝覆盖");
    });
    payload.remove("input");payload.put("inputManifest",dir.resolve("prepared/manifest.json").toString());payload.put("inputManifestSha256",manifestSha);
    ds.json.writeValue(request.toFile(),payload);
   }
   runWorker(job,"cn.sparkdq.engine.Engine",request,log,start);
   if(cluster.profile.remote()&&!terminateAndAwait(job))throw new IllegalStateException("集群清理尚未确认，保持运行中并重试核对");
   JsonNode result=ds.json.readTree(dir.resolve("result.json").toFile());
   if(!result.path("results").isArray()||result.path("results").size()!=rules.size()||!result.path("totalRows").isIntegralNumber()||result.path("totalRows").asLong()<0)throw new IllegalStateException("Spark 结果不完整");
   Path generated;
   if(policy!=null){
    try(var parts=Files.list(dir.resolve("governed"))){
     var files=parts.filter(f->f.getFileName().toString().startsWith("part-")&&f.toString().endsWith(".csv")).toList();
     if(files.size()!=1)throw new IllegalStateException("治理输出必须是单个 CSV 分片");generated=files.get(0);
    }
   }else generated=null;
   checkpoint(job);
   tx.executeWithoutResult(s->{
    // This row lock is shared with cancel(): exactly one outcome may publish durable results.
    if(!"RUNNING".equals(lockedStatus(id)))return;
    if(policy!=null){
     try(var stream=Files.newInputStream(generated)){
      var sourceDataset=ds.db.queryForMap("select dataset_id from tasks where id=?",id);
      var version=job.controlled?ds.registerControlled("governed-"+id+".csv",generated,true,Map.of("kind","GOVERNED","parentDatasetId",sourceDataset.get("dataset_id")),null):ds.register("governed-"+id+".csv",stream,true);
      long inputRows=result.path("governance").path("inputRows").asLong(-1),outputRows=((Number)version.get("row_count")).longValue();
      if(inputRows<outputRows||outputRows!=result.path("totalRows").asLong())throw new IllegalStateException("治理输出行数与复检结果不一致");
      ds.db.update("insert into governance_audits(task_id,audit_json) values(?,?)",id,result.path("governance").toString());
      var source=ds.db.queryForMap("select dataset_id from tasks where id=?",id);
      ds.db.update("insert into dataset_versions(dataset_id,parent_dataset_id,governance_task_id) values(?,?,?)",version.get("id"),source.get("dataset_id"),id);
      ds.db.update("update governance_runs set output_dataset_id=?,input_rows=?,output_rows=?,removed_rows=? where task_id=?",version.get("id"),inputRows,outputRows,inputRows-outputRows,id);
     }catch(Exception e){throw new IllegalStateException("治理版本登记失败: "+e.getMessage(),e);}
    }
    for(JsonNode row:result.path("results"))ds.db.update("insert into results(id,rule_id,metrics_json,anomaly_path) values(?,?,?,?)",Datasets.id(),row.path("ruleId").asText(),row.path("metrics").toString(),row.path("anomalyPath").asText());
    ds.db.update("update tasks set status='SUCCEEDED',total_rows=?,spark_version=?,duration_ms=?,finished_at=CURRENT_TIMESTAMP(3) where id=?",result.path("totalRows").asLong(),result.path("sparkVersion").asText(),(System.nanoTime()-start)/1000000,id);
   });
  }catch(Exception e){
   String reason=e instanceof InterruptedException?"服务停止中断任务，请重新提交":e.getMessage()==null?e.getClass().getSimpleName():e.getMessage();
   job.failureReason=reason.substring(0,Math.min(5000,reason.length()));
   if(cluster.profile.remote())ds.db.update("update tasks set error=?,duration_ms=? where id=? and status='RUNNING'",job.failureReason+"；等待集群清理核对",(System.nanoTime()-start)/1000000,id);
   else ds.db.update("update tasks set status='FAILED',error=?,duration_ms=?,finished_at=CURRENT_TIMESTAMP(3) where id=? and status='RUNNING'",reason.substring(0,Math.min(5000,reason.length())),(System.nanoTime()-start)/1000000,id);
   if(e instanceof InterruptedException)Thread.currentThread().interrupt();
  }finally{
   try{
    if(terminateAndAwait(job))finishStoppedJob(job);
    else ds.db.update("update tasks set error='取消请求已记录，子进程尚未完全退出，保持取消中',duration_ms=? where id=? and status='CANCELLING'",(System.nanoTime()-start)/1000000,id);
   }catch(Exception e){LOG.warn("Task cleanup will retry for {}",id,e);}
   finally{job.runnerFinished=true;}
  }
 }
 long workerBudgetNanos(){return TimeUnit.SECONDS.toNanos(ClusterExecution.budget(System.getenv()));}
 void runWorker(Job job,String mainClass,Path request,Path log,long start)throws Exception{
   var command=new ArrayList<String>();command.add(Path.of(System.getProperty("java.home"),"bin/java").toString());command.add("-Xmx1g");
   for(String module:List.of("java.lang","java.lang.invoke","java.lang.reflect","java.io","java.net","java.nio","java.util","java.util.concurrent","java.util.concurrent.atomic","sun.nio.ch","sun.nio.cs","sun.security.action","sun.util.calendar"))command.add("--add-opens=java.base/"+module+"=ALL-UNNAMED");
   command.addAll(List.of("-cp","/app/engine/spark-engine-0.1.0.jar:/app/engine/dependency/*",mainClass,request.toString()));
   checkpoint(job);
   long remaining=workerBudgetNanos()-(System.nanoTime()-start);
   if(remaining<=0)throw new IllegalStateException("Spark 准备与检测合计超过 "+ClusterExecution.budget(System.getenv())+" 秒");
   Process process=launchProcess(command,log);job.process=process;
   if(job.cancelled||pool.isShutdown()||Thread.currentThread().isInterrupted())signal(job);
   checkpoint(job);
   remaining=workerBudgetNanos()-(System.nanoTime()-start);
   if(job.controlled){
    while(remaining>0&&!process.waitFor(Math.min(remaining,TimeUnit.MILLISECONDS.toNanos(500)),TimeUnit.NANOSECONDS)){checkpoint(job);ControlledProfile.output(taskDirectory(job.id));remaining=workerBudgetNanos()-(System.nanoTime()-start);}
    ControlledProfile.output(taskDirectory(job.id));
   }
   if(remaining<=0||!process.waitFor(remaining,TimeUnit.NANOSECONDS))throw new IllegalStateException("Spark 准备与检测合计超过 "+ClusterExecution.budget(System.getenv())+" 秒");
   checkpoint(job);
   if(process.exitValue()!=0)throw new IllegalStateException("Spark 退出码 "+process.exitValue()+": "+tail(log));
 }
 String tail(Path file)throws Exception{String summary;try(var lines=Files.lines(file)){summary=lines.filter(line->line.contains("Exception")||line.contains("ERROR")||line.contains("Caused by")).limit(5).collect(java.util.stream.Collectors.joining("\n"));}if(summary.length()>1800)summary=summary.substring(0,1800);try(var f=new RandomAccessFile(file.toFile(),"r")){long offset=Math.max(0,f.length()-2000);f.seek(offset);byte[] b=new byte[(int)(f.length()-offset)];f.readFully(b);return summary+"\n"+new String(b,java.nio.charset.StandardCharsets.UTF_8);}}
 public List<Map<String,Object>> list(){return ds.db.queryForList("select t.*,d.name as dataset_name,g.task_id as governance_id from tasks t join datasets d on t.dataset_id=d.id left join governance_runs g on g.task_id=t.id order by t.created_at desc,t.id desc limit 200");}
 public Map<String,Object> detail(String id){
  var rows=ds.db.queryForList("select t.*,d.name as dataset_name from tasks t join datasets d on t.dataset_id=d.id where t.id=?",id);
  if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"任务不存在");var task=rows.get(0);
  var rules=ds.db.queryForList("select r.id,r.type,r.columns_json,s.id as result_id,s.metrics_json,s.anomaly_path,p.params_json from rules r left join results s on s.rule_id=r.id left join rule_parameters p on p.rule_id=r.id where r.task_id=? order by r.type,r.id",id);
  for(var rule:rules){var params=rule.remove("params_json");rule.put("params",params==null?Map.of():ds.decode(params));rule.put("definitionVersion",1);rule.put("columns",ds.decode(rule.remove("columns_json")));var metrics=rule.remove("metrics_json");rule.put("metrics",metrics==null?null:ds.decode(metrics));}
  var snapshots=ds.db.queryForList("select parent_task_id,input_sha256,semantics_version,rules_json,execution_plan_json,input_manifest_sha256,created_at from task_snapshots where task_id=?",id);
  Map<String,Object> snapshot=snapshots.isEmpty()?null:snapshots.get(0);
  if(snapshot!=null){
   snapshot.put("rules",ds.decode(snapshot.remove("rules_json")));
   Object plan=snapshot.remove("execution_plan_json");
   if(plan==null)snapshot.remove("input_manifest_sha256");
   else snapshot.put("executionInput",ds.decode(plan));
  }
  var runs=ds.db.queryForList("select source_task_id,policy_json,output_dataset_id,input_rows,output_rows,removed_rows from governance_runs where task_id=?",id);
  Map<String,Object> governance=runs.isEmpty()?null:runs.get(0);
  if(governance!=null){
   governance.put("policy",ds.decode(governance.remove("policy_json")));
   var audits=ds.db.queryForList("select audit_json from governance_audits where task_id=?",id);
   // Older tasks never recorded operation-level evidence; do not invent historical counts.
   governance.put("audit",audits.isEmpty()?null:ds.decode(audits.get(0).get("audit_json")));
  }
  var refs=ds.db.queryForList("select r.template_id,r.revision,v.name from task_template_refs r join rule_template_versions v on v.template_id=r.template_id and v.revision=r.revision where r.task_id=?",id);
  task.put("template",refs.isEmpty()?null:refs.get(0));
  task.put("governance",governance);
  task.put("snapshot",snapshot);
  task.put("execution",ExecutionReceipts.read(taskDirectory(id),id,ds.json));
  task.put("clusterExecution",cluster.detail(taskDirectory(id),id));
  task.put("rules",rules);return task;
 }
 public List<Object> preview(String taskId,String ruleId)throws Exception{
  var rows=ds.db.queryForList("select s.anomaly_path from results s join rules r on r.id=s.rule_id join tasks t on t.id=r.task_id where r.id=? and t.id=? and t.status='SUCCEEDED'",ruleId,taskId);
  if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"结果不存在");
  Path dir=Path.of(rows.get(0).get("anomaly_path").toString());var out=new ArrayList<Object>();
  try(var paths=Files.list(dir)){for(Path p:paths.filter(p->p.getFileName().toString().startsWith("part-")&&p.toString().endsWith(".json")).sorted().toList()){
   try(var lines=Files.lines(p)){var it=lines.iterator();while(it.hasNext()&&out.size()<50)out.add(CsvLimitsReader.previewObject(ds.json.readValue(it.next(),Object.class)));}if(out.size()>=50)break;
  }}return out;
 }
}
