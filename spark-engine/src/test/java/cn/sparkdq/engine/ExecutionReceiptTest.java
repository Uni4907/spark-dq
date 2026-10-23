package cn.sparkdq.engine;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import static org.junit.jupiter.api.Assertions.*;
class ExecutionReceiptTest {
 @TempDir Path folder;
 Path request()throws Exception{return Files.writeString(folder.resolve("request.json"),"{\"taskId\":\"task-1\"}");}
 @Test void recordsBindingAndGracefulDriverEndWithoutDatasetValues()throws Exception{
  var receipt=ExecutionReceipt.begin(folder,"task-1","spark://master:7077",request());receipt.started("app-20261020-1");receipt.ended(true,true);
  var json=new ObjectMapper();var begin=json.readTree(folder.resolve("execution-submitted.json").toFile());var end=json.readTree(folder.resolve("execution-ended.json").toFile());
  assertEquals(begin.path("attemptId"),end.path("attemptId"));assertEquals(64,end.path("requestSha256").asText().length());assertEquals("COMPLETED",end.path("driverOutcome").asText());assertTrue(end.path("stopReturned").asBoolean());assertEquals("app-20261020-1",receipt.binding().get("applicationId"));
 }
 @Test void refusesToOverwriteAnEarlierAttempt()throws Exception{
  Path request=request();ExecutionReceipt.begin(folder,"task-1","local[2]",request);String before=Files.readString(folder.resolve("execution-submitted.json"));
  assertThrows(FileAlreadyExistsException.class,()->ExecutionReceipt.begin(folder,"task-1","local[2]",request));assertEquals(before,Files.readString(folder.resolve("execution-submitted.json")));
  try(var files=Files.list(folder)){assertFalse(files.anyMatch(p->p.getFileName().toString().startsWith(".receipt-")));}
 }
 @Test void startupFailureIsRecordedWithoutInventingAnApplicationId()throws Exception{
  var receipt=ExecutionReceipt.begin(folder,"task-1","local[2]",request());receipt.ended(false,false);var end=new ObjectMapper().readTree(folder.resolve("execution-ended.json").toFile());assertFalse(end.has("applicationId"));assertEquals("FAILED",end.path("driverOutcome").asText());assertFalse(end.path("stopReturned").asBoolean());
 }
 @Test void rejectsUnsafeIdentityAndPreservesStartedReceipt()throws Exception{
  Path request=request();assertThrows(IllegalArgumentException.class,()->ExecutionReceipt.begin(folder,"../bad","local[2]",request));var receipt=ExecutionReceipt.begin(folder,"task-1","local[2]",request);receipt.started("app-1");assertThrows(FileAlreadyExistsException.class,()->receipt.started("app-2"));assertEquals("app-1",receipt.binding().get("applicationId"));
 }
}
