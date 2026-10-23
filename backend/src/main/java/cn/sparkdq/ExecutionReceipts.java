package cn.sparkdq;

import com.fasterxml.jackson.databind.*;
import java.nio.file.*;
import java.security.*;
import java.time.Instant;
import java.util.*;

/** Reads bounded, bound driver evidence; does not infer business success or remote cleanup. */
final class ExecutionReceipts {
 static final Set<String> BASE=Set.of("version","taskId","attemptId","requestSha256","sparkMaster");
 static Map<String,Object> read(Path folder,String taskId,ObjectMapper json){
  var names=List.of("execution-submitted.json","execution-started.json","execution-ended.json");
  if(names.stream().noneMatch(name->Files.exists(folder.resolve(name),LinkOption.NOFOLLOW_LINKS)))return Map.of("evidenceState","ABSENT");
  try{
   JsonNode submitted=load(folder.resolve(names.get(0)),json);check(submitted,Set.of("submittedAt"));
   require(submitted.path("version").isInt()&&submitted.path("version").intValue()==1);
   require(text(submitted,"taskId").equals(taskId)&&taskId.matches("[A-Za-z0-9_-]{1,128}"));
   require(UUID.fromString(text(submitted,"attemptId")).toString().equals(text(submitted,"attemptId")));
   require(text(submitted,"requestSha256").matches("[0-9a-f]{64}"));
   require(text(submitted,"sparkMaster").length()<=512);Instant.parse(text(submitted,"submittedAt"));
   Path request=folder.resolve("request.json");require(Files.isRegularFile(request,LinkOption.NOFOLLOW_LINKS)&&Files.size(request)<=1024*1024);
   var hash=MessageDigest.getInstance("SHA-256");try(var stream=new DigestInputStream(Files.newInputStream(request),hash)){stream.transferTo(java.io.OutputStream.nullOutputStream());}
   require(HexFormat.of().formatHex(hash.digest()).equals(text(submitted,"requestSha256")));
   var result=new LinkedHashMap<String,Object>();result.put("evidenceState","SUBMITTED");result.put("submitted",json.convertValue(submitted,Map.class));
   JsonNode started=null;
   if(Files.exists(folder.resolve(names.get(1)),LinkOption.NOFOLLOW_LINKS)){
    started=load(folder.resolve(names.get(1)),json);check(started,Set.of("applicationId","startedAt"));bind(submitted,started);application(started);Instant.parse(text(started,"startedAt"));
    result.put("started",json.convertValue(started,Map.class));result.put("evidenceState","STARTED");
   }
   if(Files.exists(folder.resolve(names.get(2)),LinkOption.NOFOLLOW_LINKS)){
    var ended=load(folder.resolve(names.get(2)),json);check(ended,started==null?Set.of("driverOutcome","stopReturned","endedAt"):Set.of("driverOutcome","stopReturned","endedAt","applicationId"));bind(submitted,ended);Instant.parse(text(ended,"endedAt"));
    require(ended.path("stopReturned").isBoolean()&&Set.of("COMPLETED","FAILED").contains(text(ended,"driverOutcome")));
    if(started==null)require(!ended.path("stopReturned").booleanValue()&&text(ended,"driverOutcome").equals("FAILED"));
    else require(text(started,"applicationId").equals(text(ended,"applicationId")));
    result.put("ended",json.convertValue(ended,Map.class));result.put("evidenceState","DRIVER_ENDED");
   }
   return result;
  }catch(Exception error){return Map.of("evidenceState","INVALID","reason","作业回执缺失、损坏或与任务请求不一致；未据此推断执行状态");}
 }
 static JsonNode load(Path path,ObjectMapper json)throws Exception{require(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)&&Files.size(path)<=16384);return json.readTree(path.toFile());}
 static void check(JsonNode node,Set<String> extra){require(node!=null&&node.isObject());var fields=new HashSet<String>();node.fieldNames().forEachRemaining(fields::add);var expected=new HashSet<>(BASE);expected.addAll(extra);require(fields.equals(expected));}
 static String text(JsonNode node,String key){require(node.path(key).isTextual());return node.path(key).textValue();}
 static void bind(JsonNode first,JsonNode second){for(String key:BASE)require(first.path(key).equals(second.path(key)));}
 static void application(JsonNode node){require(text(node,"applicationId").matches("[A-Za-z0-9_-]{1,128}"));}
 static void require(boolean condition){if(!condition)throw new IllegalArgumentException("Invalid execution receipt");}
}
