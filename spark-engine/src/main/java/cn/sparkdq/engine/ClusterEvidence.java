package cn.sparkdq.engine;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.scheduler.*;
import java.nio.file.*;
import java.util.*;

/** Opt-in Spark listener. Aggregates executor task events, never dataset values. */
public final class ClusterEvidence extends SparkListener {
 private final Path output;private final Map<String,Map<String,Object>> executors=new TreeMap<>();private boolean truncated;
 public ClusterEvidence(){this(path(System.getenv("SPARK_DQ_EVIDENCE")));}
 ClusterEvidence(Path output){this.output=output;}
 static Path path(String value){
  if(value==null||value.isBlank())throw new IllegalArgumentException("Explicit SPARK_DQ_EVIDENCE path required");
  Path path=Path.of(value);if(!path.isAbsolute()||!path.normalize().equals(path))throw new IllegalArgumentException("Evidence output must be absolute and normalized");return path;
 }
 synchronized void task(String id,String host,boolean success){
  if(id==null||host==null||id.length()>128||host.length()>253){truncated=true;return;}
  var entry=executors.get(id);
  if(entry==null){if(executors.size()>=64){truncated=true;return;}entry=new LinkedHashMap<>(Map.of("id",id,"host",host,"successfulTasks",0L,"failedTasks",0L));executors.put(id,entry);}
  if(!host.equals(entry.get("host"))){truncated=true;return;}
  String key=success?"successfulTasks":"failedTasks";entry.put(key,((Number)entry.get(key)).longValue()+1);
 }
 @Override public void onTaskEnd(SparkListenerTaskEnd event){task(event.taskInfo().executorId(),event.taskInfo().host(),"Success".equals(event.reason().toString()));}
 synchronized Map<String,Object> snapshot(){return Map.of("version",1,"applicationEnded",true,"truncated",truncated,"executors",new ArrayList<>(executors.values()));}
 @Override public synchronized void onApplicationEnd(SparkListenerApplicationEnd event){
  try(var stream=Files.newOutputStream(output,StandardOpenOption.CREATE_NEW)){new ObjectMapper().writeValue(stream,snapshot());}
  catch(Exception error){System.err.println("SparkDQ executor evidence was not published: "+error.getClass().getSimpleName());}
 }
}
