package cn.sparkdq;

import com.fasterxml.jackson.databind.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Fixed-topology, single Docker client process. No address is accepted from task JSON. */
final class ClusterExecution {
 record Profile(boolean remote,String master,String driver,URI masterWeb,List<URI> workers,String fingerprint){}
 interface Fetch {JsonNode get(URI uri)throws Exception;}
 final Profile profile;final ObjectMapper json;final Fetch fetch;
 final Map<String,Long> firstClear=new ConcurrentHashMap<>(),nextProbe=new ConcurrentHashMap<>();
 final Map<String,String> observations=new ConcurrentHashMap<>();
 final Set<String> confirmed=ConcurrentHashMap.newKeySet();
 ClusterExecution(Profile profile,ObjectMapper json){this(profile,json,uri->http(uri,json));}
 ClusterExecution(Profile profile,ObjectMapper json,Fetch fetch){this.profile=profile;this.json=json;this.fetch=fetch;}
 static Profile settings(Map<String,String> env){
  String mode=env.getOrDefault("SPARK_DQ_EXECUTION","local");
  if(mode.equals("local")){
   if(env.containsKey("SPARK_MASTER")&&!env.get("SPARK_MASTER").matches("local(?:\\[.*\\])?"))throw new IllegalArgumentException("Remote master requires explicit standalone profile");
   return new Profile(false,env.getOrDefault("SPARK_MASTER","local[2]"),"127.0.0.1",null,List.of(),"local");
  }
  require(mode.equals("standalone"));String master=env.get("SPARK_MASTER"),driver=env.get("SPARK_DQ_DRIVER_HOST");
  URI address=URI.create(Objects.requireNonNull(master));require(address.getScheme().equals("spark")&&address.getHost()!=null&&address.getPort()>0&&address.getPort()<=65535&&address.getRawUserInfo()==null&&address.getRawQuery()==null&&address.getRawFragment()==null&&address.getPath().isEmpty());
  require(driver!=null&&driver.length()<=253&&driver.matches("[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?")&&!driver.equalsIgnoreCase("localhost")&&!driver.startsWith("127.")&&!driver.equals("0.0.0.0"));
  URI masterWeb=web(env.get("SPARK_DQ_MASTER_JSON_URL"));
  var workers=Arrays.stream(Objects.requireNonNull(env.get("SPARK_DQ_WORKER_JSON_URLS")).split(",",-1)).map(ClusterExecution::web).toList();
  require(workers.size()>=1&&workers.size()<=4&&new HashSet<>(workers).size()==workers.size()&&!workers.contains(masterWeb));
  try{
   String canonical=master+"\n"+driver+"\n"+masterWeb+"\n"+String.join("\n",workers.stream().map(URI::toString).sorted().toList());
   String hash=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
   return new Profile(true,master,driver,masterWeb,workers,hash);
  }catch(Exception error){throw new IllegalArgumentException("Invalid fixed cluster configuration");}
 }
 static int budget(Map<String,String> env){int seconds=Integer.parseInt(env.getOrDefault("SPARK_DQ_TASK_TIMEOUT_SECONDS","300"));require(seconds>=5&&seconds<=300);return seconds;}
 static URI web(String value){URI uri=URI.create(Objects.requireNonNull(value));require(Set.of("http","https").contains(uri.getScheme())&&uri.getHost()!=null&&uri.getRawUserInfo()==null&&uri.getRawQuery()==null&&uri.getRawFragment()==null&&uri.getPath().equals("/json/")&&(uri.getPort()==-1||uri.getPort()>0&&uri.getPort()<=65535));return uri;}
 void configure(ProcessBuilder builder,List<String> command){
  var env=builder.environment();boolean engine=command.size()>1&&command.get(command.size()-2).equals("cn.sparkdq.engine.Engine");
  if(profile.remote&&engine){
   env.put("SPARK_MASTER",profile.master);env.put("SPARK_DQ_DRIVER_HOST",profile.driver);env.remove("SPARK_LOCAL_IP");
   env.put("SPARK_DQ_ENGINE_JAR","/app/engine/spark-engine-0.1.0.jar");env.put("SPARK_DQ_EVIDENCE",Path.of(command.get(command.size()-1)).toAbsolutePath().getParent().resolve("executor-evidence.json").toString());env.put("SPARK_DQ_EXECUTOR_EVIDENCE","true");
  }else{
   env.put("SPARK_MASTER",engine?profile.master:"local[2]");env.put("SPARK_LOCAL_IP","127.0.0.1");env.remove("SPARK_DQ_DRIVER_HOST");env.remove("SPARK_DQ_ENGINE_JAR");
  }
 }
 void begin(Path folder,String id)throws Exception{
  if(!profile.remote)return;
  publish(folder.resolve("cluster-intent.json"),Map.of("version",1,"taskId",id,"profileSha256",profile.fingerprint,"sparkMaster",profile.master,"createdAt",Instant.now().toString()));
 }
 boolean hasIntent(Path folder){return Files.exists(folder.resolve("cluster-intent.json"),LinkOption.NOFOLLOW_LINKS);}
 boolean foreignRemote(Path folder,String id){
  var receipt=ExecutionReceipts.read(folder,id,json);
  return receipt.get("submitted") instanceof Map<?,?> submitted&&submitted.get("sparkMaster").toString().startsWith("spark://");
 }
 boolean ready(Path folder,String id){
  if(confirmed.contains(id))return true;
  if(!hasIntent(folder)){var receipt=ExecutionReceipts.read(folder,id,json);return !receipt.get("evidenceState").equals("INVALID")&&!foreignRemote(folder,id);}
  if(!profile.remote){observations.put(id,"REQUIRES_ORIGINAL_CLUSTER_PROFILE");return false;}
  long now=System.nanoTime();if(now<nextProbe.getOrDefault(id,0L))return false;nextProbe.put(id,now+500_000_000L);
  try{
   var intent=ExecutionReceipts.load(folder.resolve("cluster-intent.json"),json);
   require(intent.isObject()&&intent.size()==5&&intent.path("version").isInt()&&intent.path("version").intValue()==1&&intent.path("taskId").asText().equals(id)&&intent.path("profileSha256").asText().equals(profile.fingerprint)&&intent.path("sparkMaster").asText().equals(profile.master));
   var receipt=ExecutionReceipts.read(folder,id,json);require(!receipt.get("evidenceState").equals("INVALID"));
   String application=null,name=null;
   if(receipt.get("submitted") instanceof Map<?,?> submitted){
    require(profile.master.equals(submitted.get("sparkMaster")));name="SparkDQ-"+id+"-"+submitted.get("attemptId");
    if(receipt.get("started") instanceof Map<?,?> started)application=started.get("applicationId").toString();
   }
   // No submitted receipt means Engine never reached Spark initialization. The local
   // process must already be dead before the caller invokes this check.
   if(name!=null&&!empty(application,name)){
    firstClear.remove(id);observations.put(id,"REMOTE_RESOURCES_PRESENT");return false;
   }
   if(name!=null){long first=firstClear.computeIfAbsent(id,k->System.nanoTime());if(System.nanoTime()-first<1_000_000_000L){observations.put(id,"AWAITING_SECOND_CLEAR_OBSERVATION");return false;}}
   var proof=new LinkedHashMap<String,Object>();proof.put("version",1);proof.put("taskId",id);proof.put("profileSha256",profile.fingerprint);proof.put("observedAt",Instant.now().toString());proof.put("state","CLEAR");if(application!=null)proof.put("applicationId",application);
   Path target=folder.resolve("cluster-cleanup.json");if(!Files.exists(target))publish(target,proof);
   else{var old=ExecutionReceipts.load(target,json);require(old.path("taskId").asText().equals(id)&&old.path("profileSha256").asText().equals(profile.fingerprint)&&old.path("state").asText().equals("CLEAR"));}
   confirmed.add(id);observations.put(id,"CLEAR");return true;
  }catch(Exception error){firstClear.remove(id);observations.put(id,"UNAVAILABLE_OR_INVALID_EVIDENCE");return false;}
 }
 boolean empty(String application,String name)throws Exception{
  JsonNode master=fetch.get(profile.masterWeb);require(master.path("url").asText().equals(profile.master)&&master.path("status").asText().equals("ALIVE"));
  require(master.path("activeapps").isArray()&&master.path("activeapps").size()<=10000&&master.path("workers").isArray()&&master.path("workers").size()<=64);
  var expected=new HashMap<String,URI>();for(URI uri:profile.workers)expected.put(uri.toString().replaceFirst("/json/$",""),uri);
  var alive=new HashMap<URI,String>();
  for(JsonNode worker:master.path("workers"))if(worker.path("state").asText().equals("ALIVE")){
   URI endpoint=expected.get(worker.path("webuiaddress").asText());require(endpoint!=null&&!alive.containsKey(endpoint)&&worker.path("id").isTextual());alive.put(endpoint,worker.path("id").asText());
  }
  require(alive.size()==profile.workers.size());
  boolean busy=false;
  for(JsonNode app:master.path("activeapps")){require(app.path("id").isTextual()&&app.path("name").isTextual());if(name.equals(app.path("name").asText())||application!=null&&application.equals(app.path("id").asText())){require(name.equals(app.path("name").asText()));busy=true;}}
  for(URI endpoint:profile.workers){
   JsonNode worker=fetch.get(endpoint);require(worker.path("masterurl").asText().equals(profile.master)&&worker.path("id").asText().equals(alive.get(endpoint))&&worker.path("executors").isArray()&&worker.path("executors").size()<=10000);
   for(JsonNode executor:worker.path("executors")){require(executor.path("appid").isTextual()&&executor.path("appdesc").path("name").isTextual());String found=executor.path("appdesc").path("name").asText();if(name.equals(found)||application!=null&&application.equals(executor.path("appid").asText())){require(name.equals(found));busy=true;}}
  }
  return !busy;
 }
 void forget(String id){confirmed.remove(id);firstClear.remove(id);nextProbe.remove(id);observations.remove(id);}
 Map<String,Object> detail(Path folder,String id){
  if(!hasIntent(folder))return Map.of("mode","local");
  try{
   var intent=ExecutionReceipts.load(folder.resolve("cluster-intent.json"),json);require(intent.path("taskId").asText().equals(id));
   String state=observations.getOrDefault(id,"PENDING");
   if(!observations.containsKey(id)&&Files.exists(folder.resolve("cluster-cleanup.json"))){var proof=ExecutionReceipts.load(folder.resolve("cluster-cleanup.json"),json);if(proof.path("taskId").asText().equals(id)&&proof.path("profileSha256").equals(intent.path("profileSha256"))&&proof.path("state").asText().equals("CLEAR"))state="CLEAR";}
   return Map.of("mode","standalone","cleanupObservation",state,"profileSha256",intent.path("profileSha256").asText());
  }catch(Exception error){return Map.of("mode","unknown","cleanupObservation","INVALID");}
 }
 static JsonNode http(URI uri,ObjectMapper json)throws Exception{
  var connection=(HttpURLConnection)uri.toURL().openConnection(Proxy.NO_PROXY);connection.setInstanceFollowRedirects(false);connection.setConnectTimeout(500);connection.setReadTimeout(500);
  long deadline=System.nanoTime()+2_000_000_000L;
  try{
   require(connection.getResponseCode()==200);
   try(var input=connection.getInputStream();var output=new java.io.ByteArrayOutputStream()){
    byte[] bytes=new byte[4096];int size;while((size=input.read(bytes))!=-1){require(System.nanoTime()<deadline&&output.size()+size<=2*1024*1024);output.write(bytes,0,size);}return json.readTree(output.toByteArray());
   }
  }finally{connection.disconnect();}
 }
 void publish(Path target,Map<String,Object> value)throws Exception{
  Path temporary=target.resolveSibling(".cluster-"+UUID.randomUUID()+".tmp");
  try{
   byte[] data=json.writeValueAsBytes(value);try(var file=FileChannel.open(temporary,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){var buffer=ByteBuffer.wrap(data);while(buffer.hasRemaining())file.write(buffer);file.force(true);}
   Files.createLink(target,temporary);Files.delete(temporary);try(var directory=FileChannel.open(target.getParent(),StandardOpenOption.READ)){directory.force(true);}
  }finally{Files.deleteIfExists(temporary);}
 }
 static void require(boolean value){if(!value)throw new IllegalArgumentException("Invalid fixed-topology cluster evidence");}
}
