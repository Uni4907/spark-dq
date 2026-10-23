package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
class ExecutionReceiptsTest {
 @TempDir Path folder;final ObjectMapper json=new ObjectMapper();
 Map<String,Object> begin()throws Exception{
  Files.writeString(folder.resolve("request.json"),"{\"taskId\":\"task\"}");var node=new LinkedHashMap<String,Object>();node.put("version",1);node.put("taskId","task");node.put("attemptId",UUID.randomUUID().toString());node.put("requestSha256",HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(folder.resolve("request.json")))));node.put("sparkMaster","local[2]");return node;
 }
 void write(String stage,Map<String,Object> base,Map<String,Object> extra)throws Exception{var node=new LinkedHashMap<>(base);node.putAll(extra);json.writeValue(folder.resolve("execution-"+stage+".json").toFile(),node);}
 String state(){return ExecutionReceipts.read(folder,"task",json).get("evidenceState").toString();}
 @Test void historicalAbsenceDoesNotInventAnApplication(){assertEquals("ABSENT",state());}
 @Test void readStagesRemainSeparateFromBusinessTaskSuccess()throws Exception{
  var base=begin();write("submitted",base,Map.of("submittedAt","2026-10-20T02:00:00Z"));assertEquals("SUBMITTED",state());
  write("started",base,Map.of("startedAt","2026-10-20T02:00:01Z","applicationId","app-1"));assertEquals("STARTED",state());
  write("ended",base,Map.of("endedAt","2026-10-20T02:00:02Z","applicationId","app-1","driverOutcome","COMPLETED","stopReturned",true));assertEquals("DRIVER_ENDED",state());assertFalse(ExecutionReceipts.read(folder,"task",json).containsKey("status"));
 }
 @Test void changedRequestOrWrongTaskIdentityInvalidatesEvidence()throws Exception{
  var base=begin();write("submitted",base,Map.of("submittedAt","2026-10-20T02:00:00Z"));assertEquals("INVALID",ExecutionReceipts.read(folder,"other",json).get("evidenceState"));Files.writeString(folder.resolve("request.json"),"changed");assertEquals("INVALID",state());
 }
 @Test void incompleteOrForeignApplicationEndCannotLookCompleted()throws Exception{
  var base=begin();write("submitted",base,Map.of("submittedAt","2026-10-20T02:00:00Z"));write("started",base,Map.of("startedAt","2026-10-20T02:00:01Z","applicationId","app-1"));write("ended",base,Map.of("endedAt","2026-10-20T02:00:02Z","applicationId","app-2","driverOutcome","COMPLETED","stopReturned",true));assertEquals("INVALID",state());Files.writeString(folder.resolve("execution-ended.json"),"{");assertEquals("INVALID",state());
 }
 @Test void boundsSymlinksAndUnexpectedFieldsAreRefused()throws Exception{
  var base=begin();write("submitted",base,Map.of("submittedAt","2026-10-20T02:00:00Z","secret","do-not-return"));assertEquals("INVALID",state());assertFalse(ExecutionReceipts.read(folder,"task",json).toString().contains("do-not-return"));
  Path path=folder.resolve("execution-submitted.json");Files.writeString(path,"x".repeat(16385));assertEquals("INVALID",state());Files.delete(path);Files.createSymbolicLink(path,folder.resolve("request.json"));assertEquals("INVALID",state());
 }
 @Test void missingApplicationCanOnlyDescribeStartupFailure()throws Exception{
  var base=begin();write("submitted",base,Map.of("submittedAt","2026-10-20T02:00:00Z"));write("ended",base,Map.of("endedAt","2026-10-20T02:00:02Z","driverOutcome","FAILED","stopReturned",false));assertEquals("DRIVER_ENDED",state());write("ended",base,Map.of("endedAt","2026-10-20T02:00:02Z","driverOutcome","COMPLETED","stopReturned",true));assertEquals("INVALID",state());
 }
}
