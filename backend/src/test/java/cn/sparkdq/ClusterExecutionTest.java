package cn.sparkdq;
import com.fasterxml.jackson.databind.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.net.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class ClusterExecutionTest {
 @TempDir Path folder;final ObjectMapper json=new ObjectMapper();
 Map<String,String> env(){return Map.of("SPARK_DQ_EXECUTION","standalone","SPARK_MASTER","spark://master:7077","SPARK_DQ_DRIVER_HOST","driver","SPARK_DQ_MASTER_JSON_URL","http://master:8080/json/","SPARK_DQ_WORKER_JSON_URLS","http://worker1:8081/json/,http://worker2:8081/json/");}
 ClusterExecution cluster(ClusterExecution.Fetch fetch){return new ClusterExecution(ClusterExecution.settings(env()),json,fetch);}
 JsonNode master()throws Exception{return json.readTree("{\"url\":\"spark://master:7077\",\"status\":\"ALIVE\",\"activeapps\":[],\"workers\":[{\"state\":\"ALIVE\",\"id\":\"w1\",\"webuiaddress\":\"http://worker1:8081\"},{\"state\":\"ALIVE\",\"id\":\"w2\",\"webuiaddress\":\"http://worker2:8081\"}]}");}
 JsonNode worker(String id)throws Exception{return json.readTree("{\"masterurl\":\"spark://master:7077\",\"id\":\""+id+"\",\"executors\":[]}");}
 ClusterExecution.Fetch clear(){return uri->uri.getHost().equals("master")?master():worker(uri.getHost().equals("worker1")?"w1":"w2");}
 void receipts()throws Exception{
  Files.writeString(folder.resolve("request.json"),"{}");var base=new LinkedHashMap<String,Object>();base.put("version",1);base.put("taskId","task");base.put("attemptId","12345678-1234-1234-1234-123456789012");base.put("sparkMaster","spark://master:7077");base.put("requestSha256",HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest("{}".getBytes())));
  var submitted=new LinkedHashMap<>(base);submitted.put("submittedAt","2026-10-20T02:00:00Z");json.writeValue(folder.resolve("execution-submitted.json").toFile(),submitted);base.put("applicationId","app-1");base.put("startedAt","2026-10-20T02:00:01Z");json.writeValue(folder.resolve("execution-started.json").toFile(),base);
 }
 @Test void profileIsExplicitAndRejectsIncompleteCredentialsOrDuplicateEndpoints(){
  assertFalse(ClusterExecution.settings(Map.of()).remote());assertThrows(IllegalArgumentException.class,()->ClusterExecution.settings(Map.of("SPARK_MASTER","spark://master:7077")));
  for(var change:List.of(Map.entry("SPARK_DQ_EXECUTION","other"),Map.entry("SPARK_DQ_MASTER_JSON_URL","http://user:pass@master:8080/json/"),Map.entry("SPARK_DQ_WORKER_JSON_URLS","http://worker1:8081/json/,http://worker1:8081/json/"),Map.entry("SPARK_DQ_DRIVER_HOST","localhost"))){var config=new HashMap<>(env());config.put(change.getKey(),change.getValue());assertThrows(RuntimeException.class,()->ClusterExecution.settings(config));}
 }
 @Test void onlyEngineReceivesRemoteEnvironment(){
  var cluster=cluster(clear());var engine=new ProcessBuilder("java");engine.environment().put("SPARK_LOCAL_IP","127.0.0.1");cluster.configure(engine,List.of("java","cn.sparkdq.engine.Engine","request"));assertEquals("spark://master:7077",engine.environment().get("SPARK_MASTER"));assertFalse(engine.environment().containsKey("SPARK_LOCAL_IP"));
  var prepare=new ProcessBuilder("java");cluster.configure(prepare,List.of("java","cn.sparkdq.engine.PreparedInput","request"));assertEquals("local[2]",prepare.environment().get("SPARK_MASTER"));assertFalse(prepare.environment().containsKey("SPARK_DQ_DRIVER_HOST"));
 }
 @Test void stoppedLocalDriverDoesNotBypassActiveWorker()throws Exception{
  var cluster=cluster(uri->{if(uri.getHost().equals("master"))return master();var node=(com.fasterxml.jackson.databind.node.ObjectNode)worker(uri.getHost().equals("worker1")?"w1":"w2");if(uri.getHost().equals("worker2"))node.putArray("executors").addObject().put("appid","app-1").putObject("appdesc").put("name","SparkDQ-task-12345678-1234-1234-1234-123456789012");return node;});
  assertFalse(cluster.empty("app-1","SparkDQ-task-12345678-1234-1234-1234-123456789012"));
 }
 @Test void masterUnreachableUnknownWorkerAndIdentityCollisionFailClosed()throws Exception{
  assertThrows(Exception.class,()->cluster(uri->{throw new java.io.IOException();}).empty("app-1","name"));
  var extra=cluster(uri->{var node=(com.fasterxml.jackson.databind.node.ObjectNode)master();node.withArray("workers").addObject().put("state","ALIVE").put("id","extra").put("webuiaddress","http://other:8081");return node;});assertThrows(IllegalArgumentException.class,()->extra.empty("app-1","name"));
  var collision=cluster(uri->{var node=(com.fasterxml.jackson.databind.node.ObjectNode)master();node.withArray("activeapps").addObject().put("id","app-1").put("name","foreign-attempt");return node;});assertThrows(IllegalArgumentException.class,()->collision.empty("app-1","name"));
 }
 @Test void workerMissingSchemaOrWrongMasterCannotCountAsClear(){
  for(String body:List.of("{}","{\"id\":\"w1\",\"masterurl\":\"spark://other:7077\",\"executors\":[]}"))assertThrows(IllegalArgumentException.class,()->cluster(uri->uri.getHost().equals("master")?master():json.readTree(body)).empty("app-1","name"));
 }
 @Test void twoClearObservationsRequiredAndProofPersists()throws Exception{
  var cluster=cluster(clear());cluster.begin(folder,"task");receipts();assertFalse(cluster.ready(folder,"task"));assertFalse(Files.exists(folder.resolve("cluster-cleanup.json")));Thread.sleep(1100);assertTrue(cluster.ready(folder,"task"));assertEquals("CLEAR",cluster.detail(folder,"task").get("cleanupObservation"));
 }
 @Test void profileMismatchAndDamagedReceiptNeverReleasePendingJob()throws Exception{
  var cluster=cluster(clear());cluster.begin(folder,"task");receipts();Files.writeString(folder.resolve("execution-started.json"),"{");assertFalse(cluster.ready(folder,"task"));
  var config=new HashMap<>(env());config.put("SPARK_DQ_DRIVER_HOST","another-driver");var other=new ClusterExecution(ClusterExecution.settings(config),json,clear());assertFalse(other.ready(folder,"task"));
 }
 @Test void localRestartKeepsRemoteIntentBlocked()throws Exception{
  var remote=cluster(clear());remote.begin(folder,"task");var local=new ClusterExecution(ClusterExecution.settings(Map.of()),json,clear());assertFalse(local.ready(folder,"task"));assertEquals("REQUIRES_ORIGINAL_CLUSTER_PROFILE",local.detail(folder,"task").get("cleanupObservation"));
 }
 @Test void intentIsCreateOnlyAndNoLaunchCanConfirmWithoutInventedApp()throws Exception{
  var cluster=cluster(uri->{throw new AssertionError("No Engine launched, no remote application possible");});cluster.begin(folder,"task");assertThrows(FileAlreadyExistsException.class,()->cluster.begin(folder,"task"));assertTrue(cluster.ready(folder,"task"));assertFalse(Files.readString(folder.resolve("cluster-cleanup.json")).contains("applicationId"));
 }
 @Test void currentFailureOverridesHistoricalClearObservation()throws Exception{
  var original=cluster(clear());original.begin(folder,"task");receipts();original.publish(folder.resolve("cluster-cleanup.json"),Map.of("taskId","task","profileSha256",original.profile.fingerprint(),"state","CLEAR"));
  var config=new HashMap<>(env());config.put("SPARK_DQ_DRIVER_HOST","changed-driver");var restarted=new ClusterExecution(ClusterExecution.settings(config),json,clear());assertFalse(restarted.ready(folder,"task"));assertEquals("UNAVAILABLE_OR_INVALID_EVIDENCE",restarted.detail(folder,"task").get("cleanupObservation"));
 }

}
