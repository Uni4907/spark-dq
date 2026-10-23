package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class ExecutionInputsTest {
 @TempDir Path root;
 final ObjectMapper json=new ObjectMapper();
 @Test void expiredSharedBudgetCannotLaunchAnotherProcess()throws Exception{
  var db=org.mockito.Mockito.mock(org.springframework.jdbc.core.JdbcTemplate.class);
  var tasks=new Tasks(new Datasets(db,json),org.mockito.Mockito.mock(org.springframework.transaction.PlatformTransactionManager.class)){
   @Override long workerBudgetNanos(){return 1;}
   @Override Process launchProcess(List<String> command,Path log){throw new AssertionError("Expired budget launched a new process");}
  };
  try{
   var job=tasks.new Job("budget","input",List.of(),"sha",null,"csv-v2");
   assertThrows(IllegalStateException.class,()->tasks.runWorker(job,"cn.sparkdq.engine.Engine",root.resolve("request.json"),root.resolve("log"),System.nanoTime()-1000));
  }finally{tasks.stop();}
 }
 @Test void legacySnapshotShapePreserved(){
  var db=org.mockito.Mockito.mock(org.springframework.jdbc.core.JdbcTemplate.class);
  org.mockito.Mockito.when(db.queryForList(org.mockito.ArgumentMatchers.startsWith("select t.*"),org.mockito.ArgumentMatchers.eq("old")))
   .thenReturn(List.of(new HashMap<>(Map.of("id","old","dataset_id","data","status","SUCCEEDED"))));
  var snapshot=new HashMap<String,Object>();snapshot.put("input_sha256","a");snapshot.put("semantics_version","csv-v1");snapshot.put("rules_json","[]");
  snapshot.put("execution_plan_json",null);snapshot.put("input_manifest_sha256",null);
  org.mockito.Mockito.when(db.queryForList(org.mockito.ArgumentMatchers.startsWith("select parent_task_id"),org.mockito.ArgumentMatchers.eq("old"))).thenReturn(List.of(snapshot));
  var tasks=new Tasks(new Datasets(db,json),org.mockito.Mockito.mock(org.springframework.transaction.PlatformTransactionManager.class));
  try{
   var result=(Map<?,?>)tasks.detail("old").get("snapshot");
   assertFalse(result.containsKey("executionInput"));assertFalse(result.containsKey("input_manifest_sha256"));assertEquals("csv-v1",result.get("semantics_version"));
  }finally{tasks.stop();}
 }
 @Test void strictShapeAndNumbers()throws Exception{
  for(String raw:List.of("[]","{}","{\"format\":\"CSV\",\"partitions\":\"4\"}","{\"format\":\"CSV\",\"partitions\":1.5}","{\"format\":\"CSV\",\"partitions\":4,\"path\":\"/etc/passwd\"}"))
   assertThrows(Exception.class,()->json.readValue(raw,ExecutionInputs.Plan.class));
  assertEquals(new ExecutionInputs.Plan("PARQUET",4),json.readValue("{\"format\":\"PARQUET\",\"partitions\":4}",ExecutionInputs.Plan.class));
 }
 @Test void validatesBoundsAndHistoricalSemantics(){
  for(var plan:List.of(new ExecutionInputs.Plan("ORC",4),new ExecutionInputs.Plan("CSV",0),new ExecutionInputs.Plan("CSV",17),new ExecutionInputs.Plan("CSV_SINGLE",4)))
   assertThrows(Exception.class,()->ExecutionInputs.freeze(plan,Map.of(),"csv-v2"));
  assertNull(ExecutionInputs.freeze(null,Map.of(),"csv-v1"));
  assertNull(ExecutionInputs.freeze(new ExecutionInputs.Plan("CSV_SINGLE",1),Map.of(),"csv-v1"));
  assertThrows(Exception.class,()->ExecutionInputs.freeze(new ExecutionInputs.Plan("PARQUET",4),Map.of(),"csv-v1"));
 }
 @Test void freezeCopiesColumnsAndRerunRefreezesNewDataset(){
  var columns=new ArrayList<>(List.of("id"));var data=new HashMap<String,Object>(Map.of("columns",columns,"row_count",5));
  var frozen=ExecutionInputs.freeze(new ExecutionInputs.Plan("CSV",4),data,"csv-v2");columns.add("name");
  assertEquals(List.of("id"),frozen.columns());assertThrows(UnsupportedOperationException.class,()->frozen.columns().add("x"));
  var parent=Map.<String,Object>of("snapshot",Map.of("executionInput",json.convertValue(frozen,Map.class)));
  assertEquals(9,ExecutionInputs.freeze(ExecutionInputs.inherited(parent),Map.of("columns",List.of("id"),"row_count",9),"csv-v2").rowCount());
  assertEquals(5,frozen.rowCount());
 }
 Map<String,Object> fixture()throws Exception{
  Files.createDirectory(root.resolve("data"));Path file=root.resolve("data/part-000.csv");Files.writeString(file,"id\n1\n");
  var node=new LinkedHashMap<String,Object>(Map.of("version",1,"format","CSV","semanticsVersion","csv-v2","sourceSha256","a".repeat(64),
   "columns",List.of("id"),"rowCount",1,"files",List.of(Map.of("path","data/part-000.csv","size",Files.size(file),"sha256",ExecutionInputs.sha(file)))));
  save(node);return node;
 }
 void save(Object node)throws Exception{
  json.writeValue(root.resolve("manifest.json").toFile(),node);
  json.writeValue(root.resolve("preparation.json").toFile(),Map.of("verified",true,"manifestSha256",ExecutionInputs.sha(root.resolve("manifest.json"))));
 }
 String verify()throws Exception{return ExecutionInputs.verify(root,new ExecutionInputs.Frozen(1,"CSV",4,1,List.of("id")),"a".repeat(64),json);}
 @Test void bindsManifestFilesAndReceipt()throws Exception{
  fixture();assertEquals(ExecutionInputs.sha(root.resolve("manifest.json")),verify());
  Files.writeString(root.resolve("data/part-000.csv"),"id\n2\n");assertThrows(Exception.class,this::verify);
 }
 @Test void resignedMetadataStillMustMatchSnapshot()throws Exception{
  var node=fixture();node.put("rowCount",2);save(node);assertThrows(Exception.class,this::verify);
  node.put("rowCount",1);node.put("sourceSha256","b".repeat(64));save(node);assertThrows(Exception.class,this::verify);
 }
 @Test void rejectsTraversalSymlinksAndReceiptTampering()throws Exception{
  var node=fixture();Object files=node.get("files");node.put("files",List.of(Map.of("path","../part-000.csv")));save(node);assertThrows(Exception.class,this::verify);
  node.put("files",files);save(node);
  Path file=root.resolve("data/part-000.csv");Files.move(file,root.resolve("other"));Files.createSymbolicLink(file,root.resolve("other"));
  assertThrows(Exception.class,this::verify);
  json.writeValue(root.resolve("preparation.json").toFile(),Map.of("verified",false));assertThrows(Exception.class,this::verify);
 }
}
