package cn.sparkdq.engine;

import com.fasterxml.jackson.databind.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;
import java.nio.file.*;
import java.util.*;

/** Standalone JVM. All quality detection runs through Spark SQL / DataFrames. */
public final class Engine {
 static long sum(Dataset<Row> df,Column predicate){return ((Number)df.agg(coalesce(org.apache.spark.sql.functions.sum(when(predicate,1L).otherwise(0L)),lit(0L))).first().get(0)).longValue();}
 // Explicit separators disable Univocity's normalization inside quoted values.
 // Dataset headers contain only validated identifiers, so their first terminator
 // determines the record separator without examining any business value.
 static String recordSeparator(String path){
  Path input=Path.of(path);
  if(Files.isDirectory(input,LinkOption.NOFOLLOW_LINKS)){
   String separator=null;
   try(var files=Files.list(input)){
    var parts=files.filter(p->{String name=p.getFileName().toString();return !name.startsWith("_")&&!name.startsWith(".")&&Files.isRegularFile(p,LinkOption.NOFOLLOW_LINKS);}).iterator();
    while(parts.hasNext()){
     String current=recordSeparator(parts.next().toString());
     if(separator!=null&&!separator.equals(current))throw new IllegalArgumentException("CSV parts use different record separators");
     separator=current;
    }
   }catch(java.io.IOException error){throw new java.io.UncheckedIOException(error);}
   if(separator==null)throw new IllegalArgumentException("CSV directory has no data files");
   return separator;
  }
  try(var in=Files.newInputStream(Path.of(path))){
   int value;while((value=in.read())!=-1){
    if(value=='\n')return "\n";
    if(value=='\r')return in.read()=='\n'?"\r\n":"\r";
   }
   return "\n";
  }catch(java.io.IOException error){throw new java.io.UncheckedIOException(error);}
 }
 static Dataset<Row> readCsv(SparkSession spark,String path,boolean rfc4180){
  var reader=spark.read().option("header","true").option("inferSchema","false")
   .option("multiLine","true").option("mode","FAILFAST").option("encoding","UTF-8")
   .option("lineSep",recordSeparator(path))
   .option("nullValue","\\N").option("emptyValue","").option("ignoreLeadingWhiteSpace","false")
   .option("ignoreTrailingWhiteSpace","false");
  if(rfc4180)reader=reader.option("escape","\"");
  return reader.csv(path);
 }
 /** Count only actual value differences; each stage uses one parallel aggregate. */
 static Dataset<Row> transformFields(Dataset<Row> df,JsonNode config,boolean fill,List<Map<String,Object>> audit){
  if(config.isMissingNode()||config.isNull())return df;
  if(!config.isObject())throw new IllegalArgumentException("Governance transformations must be objects");
  var operations=new TreeMap<String,String>();
  config.fields().forEachRemaining(entry->{
   if(!entry.getValue().isTextual())throw new IllegalArgumentException("Governance values must be strings");
   operations.put(entry.getKey(),entry.getValue().asText());
  });
  if(operations.isEmpty())return df;
  var columns=Arrays.asList(df.columns());
  if(!columns.containsAll(operations.keySet()))throw new IllegalArgumentException("Unknown governance column");
  var replacements=new LinkedHashMap<String,Column>();var measures=new ArrayList<Column>();
  for(var entry:operations.entrySet()){
   Column original=col(entry.getKey());Column replacement;
   if(fill)replacement=when(original.isNull().or(length(trim(original)).equalTo(0)),lit(entry.getValue())).otherwise(original);
   else replacement=switch(entry.getValue()){
    case "TRIM" -> trim(original);
    case "UPPER" -> upper(original);
    case "LOWER" -> lower(original);
    case "TRIM_UPPER" -> upper(trim(original));
    case "TRIM_LOWER" -> lower(trim(original));
    default -> throw new IllegalArgumentException("Unknown normalization operation");
   };
   replacements.put(entry.getKey(),replacement);
   measures.add(coalesce(org.apache.spark.sql.functions.sum(when(not(original.eqNullSafe(replacement)),1L).otherwise(0L)),lit(0L)).alias("__dq_changed_"+measures.size()));
  }
  Row changes=df.agg(measures.get(0),measures.subList(1,measures.size()).toArray(Column[]::new)).first();int index=0;
  for(var entry:operations.entrySet())audit.add(Map.of("column",entry.getKey(),"operation",fill?"FILL_MISSING":entry.getValue(),"changedRows",((Number)changes.get(index++)).longValue()));
  // A projection preserves the original column order and every unselected value.
  return df.select(columns.stream().map(name->replacements.getOrDefault(name,col(name)).alias(name)).toArray(Column[]::new));
 }
 /** Simultaneous exact substitutions: every branch compares the original stage value. */
 static Dataset<Row> mapValues(Dataset<Row> df,JsonNode config,List<Map<String,Object>> audit){
  if(config.isMissingNode()||config.isNull()||config.size()==0)return df;
  if(!config.isObject())throw new IllegalArgumentException("Mappings must be an object");
  var names=new TreeSet<String>();config.fieldNames().forEachRemaining(names::add);
  var columns=Arrays.asList(df.columns());
  if(!columns.containsAll(names))throw new IllegalArgumentException("Unknown mapping column");
  var replacements=new LinkedHashMap<String,Column>();var measures=new ArrayList<Column>();
  for(String name:names){
   JsonNode pairs=config.path(name);
   if(!pairs.isArray()||pairs.size()==0||pairs.size()>50)throw new IllegalArgumentException("Invalid mapping list");
   Column original=col(name),replacement=original,matched=lit(false);
   for(JsonNode pair:pairs){
    if(!pair.path("from").isTextual()||!pair.path("to").isTextual())throw new IllegalArgumentException("Mapping values must be text");
    Column condition=original.equalTo(pair.path("from").asText());
    matched=matched.or(condition);
    replacement=when(condition,lit(pair.path("to").asText())).otherwise(replacement);
   }
   replacements.put(name,replacement);
   measures.add(coalesce(org.apache.spark.sql.functions.sum(when(not(original.eqNullSafe(replacement)),1L).otherwise(0L)),lit(0L)));
   measures.add(coalesce(org.apache.spark.sql.functions.sum(when(matched,1L).otherwise(0L)),lit(0L)));
   for(JsonNode pair:pairs)measures.add(coalesce(org.apache.spark.sql.functions.sum(when(original.equalTo(pair.path("from").asText()),1L).otherwise(0L)),lit(0L)));
  }
  for(int i=0;i<measures.size();i++)measures.set(i,measures.get(i).alias("__dq_mapping_"+i));
  Row counts=df.agg(measures.get(0),measures.subList(1,measures.size()).toArray(Column[]::new)).first();int index=0;
  for(String name:names){
   long changed=((Number)counts.get(index++)).longValue(),matched=((Number)counts.get(index++)).longValue();
   var entries=new ArrayList<Map<String,Object>>();
   for(JsonNode pair:config.path(name)){
    long hits=((Number)counts.get(index++)).longValue();
    entries.add(Map.of("from",pair.path("from").asText(),"to",pair.path("to").asText(),"matchedRows",hits,
      "changedRows",pair.path("from").asText().equals(pair.path("to").asText())?0L:hits));
   }
   audit.add(Map.of("column",name,"operation","VALUE_MAPPING","changedRows",changed,"matchedRows",matched,"entries",entries));
  }
  return df.select(columns.stream().map(name->replacements.getOrDefault(name,col(name)).alias(name)).toArray(Column[]::new));
 }
 /** Resolve typed priorities after transforms/drop. Never silently rank invalid nonblank values. */
 static List<Column> retentionOrder(Dataset<Row> df,JsonNode config){
  var order=new ArrayList<Column>();
  if(!config.isArray()||config.size()==0||config.size()>5)throw new IllegalArgumentException("Invalid deduplication order");
  var seen=new HashSet<String>();var columns=Arrays.asList(df.columns());
  for(JsonNode item:config){
   String name=item.path("column").asText(),type=item.path("type").asText(),direction=item.path("direction").asText(),nulls=item.path("nulls").asText();
   if(!columns.contains(name)||!seen.add(name)||!Set.of("ASC","DESC").contains(direction)||!Set.of("FIRST","LAST").contains(nulls))
    throw new IllegalArgumentException("Invalid retention order field/direction/nulls");
   Column raw=col(name),present=raw.isNotNull().and(length(trim(raw)).gt(0)),value,valid;
   String quoted=String.valueOf((char)96)+name+(char)96;
   switch(type){
    case "TEXT" -> {value=raw;valid=lit(true);}
    case "DECIMAL" -> {
     value=expr("try_cast("+quoted+" as decimal(38,10))");
     valid=raw.rlike("^[+-]?(?:[0-9]{1,28}(?:\\.[0-9]{1,10})?|\\.[0-9]{1,10})$").and(value.isNotNull());
    }
    case "DATE" -> {
     value=expr("try_cast("+quoted+" as date)");
     valid=raw.rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$").and(value.isNotNull());
    }
    default -> throw new IllegalArgumentException("Invalid retention order type");
   }
   if(!type.equals("TEXT")&&df.filter(present.and(not(valid))).limit(1).count()>0)
    throw new IllegalArgumentException("保留优先级字段 "+name+" 存在非空且不合法的 "+type+" 值，未生成治理版本");
   Column key=when(present,value).otherwise(lit(null));
   order.add(direction.equals("ASC")?(nulls.equals("FIRST")?key.asc_nulls_first():key.asc_nulls_last()):(nulls.equals("FIRST")?key.desc_nulls_first():key.desc_nulls_last()));
  }
  // Stable value-level tie break, preserving the historical full-row lexical convention.
  Arrays.stream(df.columns()).forEach(name->order.add(col(name).asc_nulls_first()));
  return order;
 }
 static void rejectTransformedNullMarker(Dataset<Row> df,List<Map<String,Object>> audit){
  if(audit.isEmpty())return;
  var fields=new TreeSet<String>();audit.forEach(operation->fields.add((String)operation.get("column")));
  Column reserved=lit(false);
  for(String field:fields)reserved=reserved.or(col(field).isNotNull().and(col(field).equalTo("\\N")));
  // CSV uses this exact string as its null sentinel; reject before saving a lossy version.
  if(df.filter(reserved).limit(1).count()>0)throw new IllegalArgumentException("标准化产生保留空值标记 \\N，已停止治理，请调整配置 (reserved CSV null marker)");
 }
 public static void main(String[] args)throws Exception{
  long workerStarted=System.nanoTime();var settings=WorkerMetrics.settings(System.getenv());var network=DriverNetwork.settings(System.getenv());
  ObjectMapper json=new ObjectMapper();JsonNode request=json.readTree(Path.of(args[0]).toFile());
  Path output=Path.of(request.path("output").asText());Files.createDirectories(output);
  var receipt=ExecutionReceipt.begin(output,request.path("taskId").asText(),network.master(),Path.of(args[0]));
  boolean completed=false;SparkSession spark=null;
  try{
  long startupStarted=System.nanoTime();
  var builder=SparkSession.builder().appName(receipt.applicationName())
   .master(network.master())
   .config("spark.sql.caseSensitive","true").config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions",Integer.toString(settings.shufflePartitions()))
   .config("spark.driver.bindAddress",network.bind()).config("spark.driver.host",network.host());
  if(network.master().startsWith("spark://")&&System.getenv("SPARK_DQ_ENGINE_JAR")!=null){
   Path jar=Path.of(System.getenv("SPARK_DQ_ENGINE_JAR"));if(!jar.isAbsolute()||!Files.isRegularFile(jar))throw new IllegalArgumentException("Explicit client engine jar required");
   builder.config("spark.jars",jar.toString()).config("spark.executor.memory","768m").config("spark.executor.cores","1").config("spark.cores.max","2").config("spark.dynamicAllocation.enabled","false").config("spark.scheduler.minRegisteredResourcesRatio","1").config("spark.scheduler.maxRegisteredResourcesWaitingTime","30s");
  }
  if("true".equals(System.getenv("SPARK_DQ_EXECUTOR_EVIDENCE")))builder.config("spark.extraListeners","cn.sparkdq.engine.ClusterEvidence");
  spark=builder.getOrCreate();
  long startupMs=WorkerMetrics.millisSince(startupStarted);
  receipt.started(spark.sparkContext().applicationId());
  spark.sparkContext().setLogLevel("WARN");
   long readStarted=System.nanoTime();
   PreparedInput.Manifest prepared=null;
   Dataset<Row> df;
   if(request.has("inputManifest")){
    PreparedInput.require(!request.has("input")&&"csv-v2".equals(request.path("semanticsVersion").asText()),"Prepared input requires an exclusive manifest and csv-v2");
    prepared=PreparedInput.load(Path.of(request.path("inputManifest").asText()),request.path("inputManifestSha256").asText());
    df=PreparedInput.read(spark,prepared);
   }else{
    PreparedInput.require(!request.has("inputManifestSha256"),"Manifest fingerprint requires a manifest");
    df=readCsv(spark,request.path("input").asText(),!"csv-v1".equals(request.path("semanticsVersion").asText("csv-v1")));
   }
   int sourcePartitions=df.rdd().getNumPartitions();
   boolean repartitionApplied=settings.inputPartitions()>0&&sourcePartitions!=settings.inputPartitions();
   if(repartitionApplied)df=df.repartition(settings.inputPartitions());
   df=df.cache();int observedInputPartitions=df.rdd().getNumPartitions();
   long inputRows=df.count();
   if(prepared!=null){
    PreparedInput.require(inputRows==prepared.rows(),"Prepared row count mismatch");
    // The saved governance version remains CSV. Reject a non-null Parquet sentinel before it could become null.
    if(prepared.format().equals("PARQUET")){
     Column reserved=lit(false);
     for(String name:df.columns())reserved=reserved.or(col(name).equalTo("\\N"));
     PreparedInput.require(df.filter(reserved).limit(1).count()==0,"Prepared Parquet contains reserved CSV null marker");
    }
   }
   long droppedMissingRows=0;Map<String,Object> retentionAudit=null;var transformations=new ArrayList<Map<String,Object>>();
   long readMs=WorkerMetrics.millisSince(readStarted),governanceStarted=System.nanoTime();
   if(request.has("governance")){
    JsonNode policy=request.path("governance");
    // v2 remains normalize/fill/drop/dedup; v3 adds one simultaneous mapping stage.
    if(policy.hasNonNull("mappings")&&policy.path("mappings").size()>0&&!Set.of(3,4).contains(policy.path("policyVersion").asInt()))
     throw new IllegalArgumentException("Mappings require policy version 3 or 4");
    JsonNode priority=policy.path("deduplicationOrder");
    if(priority.size()>0&&(policy.path("policyVersion").asInt()!=4||!policy.path("deduplicate").asBoolean()))
     throw new IllegalArgumentException("Retention order requires v4 and deduplication");
    // Missing maps preserve historical governance requests unchanged.
    df=transformFields(df,policy.path("normalizations"),false,transformations);
    df=mapValues(df,policy.path("mappings"),transformations);
    df=transformFields(df,policy.path("fills"),true,transformations);
    rejectTransformedNullMarker(df,transformations);
    if(policy.path("dropMissing").asBoolean()){
     for(JsonNode rule:request.path("rules"))if("MISSING".equals(rule.path("type").asText())){
      Column c=col(rule.path("columns").get(0).asText());df=df.filter(c.isNotNull().and(length(trim(c)).gt(0)));
     }
     droppedMissingRows=inputRows-df.count();
    }
    if(policy.path("deduplicate").asBoolean()){
     JsonNode duplicate=null;
     for(JsonNode rule:request.path("rules"))if("DUPLICATE".equals(rule.path("type").asText())){if(duplicate!=null)throw new IllegalArgumentException("Multiple duplicate rules");duplicate=rule;}
     if(duplicate==null)throw new IllegalArgumentException("Missing duplicate rule");
     var keys=new ArrayList<Column>();duplicate.path("columns").forEach(c->keys.add(col(c.asText())));
     Column[] order=priority.size()>0?retentionOrder(df,priority).toArray(Column[]::new):Arrays.stream(df.columns()).map(c->col(c).asc_nulls_first()).toArray(Column[]::new);
     if(priority.size()>0){
      long before=df.count();
      long groups=df.groupBy(keys.toArray(Column[]::new)).agg(count(lit(1)).alias("__dq_priority_count")).filter(col("__dq_priority_count").gt(1)).count();
      retentionAudit=Map.of("order",json.convertValue(priority,List.class),"tieBreak","FULL_ROW_LEXICOGRAPHIC_ASC_NULLS_FIRST","inputRows",before,"duplicateGroups",groups);
     }
     df=df.withColumn("__dq_row_number",row_number().over(Window.partitionBy(keys.toArray(Column[]::new)).orderBy(order)))
      .filter(col("__dq_row_number").equalTo(1)).drop("__dq_row_number");
    }
    df=df.cache();
    // A fresh directory and errorifexists preserve every source file and earlier version.
    df.coalesce(1).write().mode("errorifexists").option("header","true").option("nullValue","\\N")
     .option("emptyValue","\"\"").option("escape","\"").option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace","false").csv(output.resolve("governed").toString());
    // Recheck the serialized version, so its statistics describe the actual saved CSV.
    df=readCsv(spark,output.resolve("governed").toString(),true).cache();
   }
   long total=df.count();long governanceMs=WorkerMetrics.millisSince(governanceStarted),rulesStarted=System.nanoTime();var results=new ArrayList<Map<String,Object>>();
   for(JsonNode rule:request.path("rules")){
    String ruleId=rule.path("id").asText();String type=rule.path("type").asText();
    var fields=new ArrayList<String>();rule.path("columns").forEach(n->fields.add(n.asText()));
    String anomaly=output.resolve("anomalies").resolve(ruleId).toString();
    long algorithmStarted=System.nanoTime();Map<String,Object> metrics=new LinkedHashMap<>();Dataset<Row> bad;
    if(type.equals("MISSING")){
     Column c=col(fields.get(0));
     long nulls=sum(df,c.isNull());long empty=sum(df,c.isNotNull().and(c.equalTo("")));
     long whitespace=sum(df,c.isNotNull().and(length(c).gt(0)).and(length(trim(c)).equalTo(0)));
     metrics.put("nullCount",nulls);metrics.put("emptyCount",empty);metrics.put("whitespaceCount",whitespace);
     metrics.put("blankCount",nulls+empty+whitespace);
     bad=df.filter(c.isNull().or(length(trim(c)).equalTo(0)));
    }else if(type.equals("DUPLICATE")){
     Column[] keys=fields.stream().map(org.apache.spark.sql.functions::col).toArray(Column[]::new);
     Dataset<Row> groups=df.groupBy(keys).agg(count(lit(1)).alias("__dq_count")).filter(col("__dq_count").gt(1));
     long groupCount=groups.count();
     long members=((Number)groups.agg(coalesce(org.apache.spark.sql.functions.sum("__dq_count"),lit(0L))).first().get(0)).longValue();
     metrics.put("duplicateGroups",groupCount);metrics.put("duplicateRows",members);metrics.put("excessRows",members-groupCount);
     bad=df.withColumn("__dq_group_size",count(lit(1)).over(Window.partitionBy(keys))).filter(col("__dq_group_size").gt(1));
    }else if(type.equals("FD")){
     var ys=new ArrayList<String>();rule.path("params").path("dependentColumns").forEach(v->ys.add(v.asText()));
     var evaluation=FdDetection.detect(df,fields,ys,total);metrics.putAll(evaluation.metrics());bad=evaluation.anomalies();
    }else if(type.equals("IQR")){
     var evaluation=IqrDetection.detect(df,fields.get(0),rule.path("params").path("k").asText("1.5"),total);
     metrics.putAll(evaluation.metrics());bad=evaluation.anomalies();
    }else if(Set.of("TYPE","RANGE","REGEX").contains(type)){
     Column c=col(fields.get(0));JsonNode params=rule.path("params");
     String columnName=String.valueOf((char)96)+fields.get(0)+(char)96;
     Column eligible=c.isNotNull().and(length(trim(c)).gt(0));
     Column valid;
     Column decimal=expr("try_cast("+columnName+" as decimal(38,10))");
     Column numeric=c.rlike("^[+-]?(?:[0-9]{1,28}(?:\\.[0-9]{1,10})?|\\.[0-9]{1,10})$").and(decimal.isNotNull());
     if("TYPE".equals(type)){
      valid=switch(params.path("dataType").asText()){
       case "INTEGER" -> c.rlike("^[+-]?[0-9]+$").and(expr("try_cast("+columnName+" as bigint)").isNotNull());
       case "DECIMAL" -> numeric;
       case "DATE" -> c.rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$").and(expr("try_cast("+columnName+" as date)").isNotNull());
       default -> throw new IllegalArgumentException("Unknown data type");
      };
     }else if("RANGE".equals(type)){
      valid=numeric;
      if(params.has("min"))valid=valid.and(decimal.geq(lit(new java.math.BigDecimal(params.path("min").asText()))));
      if(params.has("max"))valid=valid.and(decimal.leq(lit(new java.math.BigDecimal(params.path("max").asText()))));
      metrics.put("nonNumericCount",sum(df,eligible.and(not(numeric))));
     }else valid=c.rlike("\\A(?:"+params.path("pattern").asText()+")\\z");
     Column invalid=eligible.and(not(valid));long checked=sum(df,eligible);
     metrics.put("invalidCount",sum(df,invalid));metrics.put("checkedCount",checked);metrics.put("skippedBlankCount",total-checked);
     bad=df.filter(invalid);
    }else throw new IllegalArgumentException("Unknown rule type "+type);
    // JSON lines preserve quoted newlines and distinguish strings from null; no CSV formula execution.
    long algorithmMs=WorkerMetrics.millisSince(algorithmStarted),writeStarted=System.nanoTime();
    bad.write().mode("errorifexists").option("ignoreNullFields","false").json(anomaly);
    results.add(Map.of("ruleId",ruleId,"metrics",metrics,"anomalyPath",anomaly,"algorithmMs",algorithmMs,"anomalyWriteMs",WorkerMetrics.millisSince(writeStarted)));
   }
   long rulesMs=WorkerMetrics.millisSince(rulesStarted);
   var execution=new LinkedHashMap<String,Object>(WorkerMetrics.resources());
   execution.put("sparkMaster",spark.sparkContext().master());
   execution.put("receipt",receipt.binding());
   execution.put("inputFormat",prepared==null?"CSV_SINGLE":prepared.format());
   execution.put("inputFiles",prepared==null?1:prepared.files().length);
   if(prepared!=null)execution.put("inputManifestSha256",request.path("inputManifestSha256").asText());
   execution.put("sourcePartitions",sourcePartitions);execution.put("repartitionApplied",repartitionApplied);
   execution.put("requestedInputPartitions",settings.inputPartitions());
   execution.put("inputPartitions",observedInputPartitions);execution.put("resultPartitions",df.rdd().getNumPartitions());
   execution.put("shufflePartitions",settings.shufflePartitions());
   execution.put("adaptiveEnabled",Boolean.parseBoolean(spark.conf().get("spark.sql.adaptive.enabled")));
   execution.put("sparkStartupMs",startupMs);execution.put("inputReadAndCountMs",readMs);
   execution.put("governanceAndOutputCountMs",governanceMs);execution.put("rulesAndAnomalyWriteMs",rulesMs);
   execution.put("workerElapsedMs",WorkerMetrics.millisSince(workerStarted));
   df.unpersist();
   var result=new LinkedHashMap<String,Object>();result.put("totalRows",total);result.put("sparkVersion",spark.version());result.put("results",results);result.put("execution",execution);
   if(request.has("governance")){
    var governance=new LinkedHashMap<String,Object>();governance.put("policyVersion",request.path("governance").path("policyVersion").asInt(1));
    governance.put("inputRows",inputRows);governance.put("outputRows",total);governance.put("removedRows",inputRows-total);
    governance.put("droppedMissingRows",droppedMissingRows);governance.put("deduplicatedRows",request.path("governance").path("deduplicate").asBoolean()?inputRows-droppedMissingRows-total:0L);
    if(retentionAudit!=null)governance.put("deduplication",retentionAudit);
    governance.put("transformations",transformations);result.put("governance",governance);
   }
   Path tmp=output.resolve("result.json.tmp");json.writeValue(tmp.toFile(),result);
   Files.move(tmp,output.resolve("result.json"),StandardCopyOption.ATOMIC_MOVE);
   completed=true;
  }finally{
   boolean stopReturned=false;
   try{if(spark!=null){spark.stop();stopReturned=true;}}
   finally{receipt.ended(completed,stopReturned);}
  }
 }
}
