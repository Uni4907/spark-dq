package cn.sparkdq.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.*;
import java.time.Instant;
import java.util.*;

/** Create-only durable driver receipts. A driver receipt never replaces business task state. */
final class ExecutionReceipt {
 private final Path folder;private final Map<String,Object> identity;
 private String applicationId;
 private ExecutionReceipt(Path folder,Map<String,Object> identity){this.folder=folder;this.identity=identity;}
 static ExecutionReceipt begin(Path folder,String taskId,String master,Path request)throws Exception{
  if(taskId==null||!taskId.matches("[A-Za-z0-9_-]{1,128}"))throw new IllegalArgumentException("Invalid execution task identity");
  var hash=MessageDigest.getInstance("SHA-256");
  try(var input=new DigestInputStream(Files.newInputStream(request),hash)){input.transferTo(java.io.OutputStream.nullOutputStream());}
  var identity=new LinkedHashMap<String,Object>();identity.put("version",1);identity.put("taskId",taskId);
  identity.put("attemptId",UUID.randomUUID().toString());identity.put("requestSha256",HexFormat.of().formatHex(hash.digest()));identity.put("sparkMaster",master);
  var receipt=new ExecutionReceipt(folder,identity);receipt.publish("execution-submitted.json",Map.of("submittedAt",Instant.now().toString()));return receipt;
 }
 void started(String id)throws Exception{
  if(id==null||!id.matches("[A-Za-z0-9_-]{1,128}"))throw new IllegalArgumentException("Invalid Spark application identity");
  publish("execution-started.json",Map.of("applicationId",id,"startedAt",Instant.now().toString()));applicationId=id;
 }
 void ended(boolean completed,boolean stopReturned)throws Exception{
  var fields=new LinkedHashMap<String,Object>();fields.put("driverOutcome",completed?"COMPLETED":"FAILED");
  fields.put("stopReturned",stopReturned);fields.put("endedAt",Instant.now().toString());if(applicationId!=null)fields.put("applicationId",applicationId);
  publish("execution-ended.json",fields);
 }
 String applicationName(){return "SparkDQ-"+identity.get("taskId")+"-"+identity.get("attemptId");}
 Map<String,Object> binding(){var result=new LinkedHashMap<>(identity);if(applicationId!=null)result.put("applicationId",applicationId);return result;}
 void publish(String name,Map<String,Object> fields)throws Exception{
  var node=new LinkedHashMap<>(identity);node.putAll(fields);byte[] data=new ObjectMapper().writeValueAsBytes(node);
  Path temporary=folder.resolve(".receipt-"+UUID.randomUUID()+".tmp");
  try{
   try(var stream=FileChannel.open(temporary,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){
    ByteBuffer bytes=ByteBuffer.wrap(data);while(bytes.hasRemaining())stream.write(bytes);stream.force(true);
   }
   // Link is atomic and fails if the destination exists; ATOMIC_MOVE may replace it on Linux.
   Files.createLink(folder.resolve(name),temporary);Files.delete(temporary);
   try(var directory=FileChannel.open(folder,StandardOpenOption.READ)){directory.force(true);}
  }finally{Files.deleteIfExists(temporary);}
 }
}
