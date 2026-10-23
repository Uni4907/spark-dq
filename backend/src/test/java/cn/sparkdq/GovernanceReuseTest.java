package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
class GovernanceReuseTest {
 final List<String> columns=List.of("name","status","score");
 final List<Map<String,Object>> rules=List.of(Map.of("type","MISSING"),Map.of("type","DUPLICATE"));
 Tasks.GovernanceRequest policy(){return new Tasks.GovernanceRequest(true,true,Map.of("name","  中文\n值  "),Map.of("status","TRIM_UPPER"),Map.of("status",List.of(new Tasks.ValueMapping("","ACTIVE"))),List.of(new Tasks.SortSpec("score","DECIMAL","DESC","LAST")),4);}
 Map<String,Object> task(){return new HashMap<>(Map.of("status","SUCCEEDED","dataset_id","input","dataset_name","样本.csv","created_at","2026-10-20","rules",rules,"snapshot",Map.of("semantics_version","csv-v2")));}
 @Test void copyPreservesLiteralValuesMappingsAndRetentionOrder(){
  var value=GovernanceReuse.compatible(policy(),columns,rules);assertEquals(policy(),value);assertEquals("  中文\n值  ",value.fills().get("name"));assertEquals("",value.mappings().get("status").get(0).from());assertEquals("score",value.deduplicationOrder().get(0).column());assertThrows(UnsupportedOperationException.class,()->value.fills().put("name","changed"));
 }
 @Test void missingFieldsAndRequiredTargetRulesReject(){
  assertThrows(ResponseStatusException.class,()->GovernanceReuse.compatible(policy(),List.of("name"),rules));
  assertTrue(assertThrows(ResponseStatusException.class,()->GovernanceReuse.compatible(policy(),columns,List.of(Map.of("type","DUPLICATE")))).getReason().contains("空值规则"));
  assertTrue(assertThrows(ResponseStatusException.class,()->GovernanceReuse.compatible(policy(),columns,List.of(Map.of("type","MISSING")))).getReason().contains("重复规则"));
 }
 @Test void prepareUsesEffectiveTargetVersionAndDoesNotWriteOrMutateSource(){
  var db=mock(JdbcTemplate.class);var ds=spy(new Datasets(db,new ObjectMapper()));var tasks=mock(Tasks.class);var source=task();source.put("governance",Map.of("output_dataset_id","source-output","policy",policy()));var target=task();target.put("governance",Map.of("output_dataset_id","target-output"));String before=ds.encode(source);
  when(tasks.detail("source")).thenReturn(source);when(tasks.detail("target")).thenReturn(target);when(tasks.semantics(target)).thenReturn("csv-v2");when(tasks.effectiveDataset(target)).thenReturn("target-output");doReturn(Map.of("id","target-output","columns",columns)).when(ds).one("target-output");
  var result=new GovernanceReuse(tasks,ds).prepare("source","target");assertEquals("target-output",result.get("targetDatasetId"));assertEquals(policy(),result.get("policy"));assertEquals(before,ds.encode(source));verifyNoInteractions(db);
 }
 @Test void onlySuccessfulGovernanceCanBeAReusableSource(){
  var ds=new Datasets(mock(JdbcTemplate.class),new ObjectMapper());var tasks=mock(Tasks.class);when(tasks.detail("target")).thenReturn(task());
  for(String status:List.of("SUCCEEDED","FAILED","CANCELLED","RUNNING")){var source=task();source.put("status",status);if(!status.equals("SUCCEEDED"))source.put("governance",Map.of("output_dataset_id","output","policy",policy()));when(tasks.detail("source")).thenReturn(source);assertEquals(409,assertThrows(ResponseStatusException.class,()->new GovernanceReuse(tasks,ds).prepare("source","target")).getStatusCode().value());}
 }
 @Test void unfinishedOrHistoricalTargetCannotImport(){
  var ds=new Datasets(mock(JdbcTemplate.class),new ObjectMapper());var tasks=mock(Tasks.class);var source=task();source.put("governance",Map.of("output_dataset_id","output","policy",policy()));when(tasks.detail("source")).thenReturn(source);
  for(String status:List.of("RUNNING","FAILED","SUCCEEDED")){var target=task();target.put("status",status);when(tasks.detail("target")).thenReturn(target);when(tasks.semantics(target)).thenReturn("csv-v1");assertEquals(409,assertThrows(ResponseStatusException.class,()->new GovernanceReuse(tasks,ds).prepare("source","target")).getStatusCode().value());}
 }
 @Test void damagedStoredPolicyReturnsConflict(){
  var ds=spy(new Datasets(mock(JdbcTemplate.class),new ObjectMapper()));var tasks=mock(Tasks.class);var source=task();source.put("governance",Map.of("output_dataset_id","output","policy",Map.of("unknown","bad")));var target=task();when(tasks.detail("source")).thenReturn(source);when(tasks.detail("target")).thenReturn(target);when(tasks.semantics(target)).thenReturn("csv-v2");when(tasks.effectiveDataset(target)).thenReturn("input");doReturn(Map.of("id","input","columns",columns)).when(ds).one("input");assertEquals(409,assertThrows(ResponseStatusException.class,()->new GovernanceReuse(tasks,ds).prepare("source","target")).getStatusCode().value());
 }
 @Test void successfulPoliciesUseLiteralSearchAndStableBoundedPaging(){
  var db=mock(JdbcTemplate.class);String query="x%_' OR 1=1";when(db.queryForObject(anyString(),eq(Long.class),eq(query),eq(query))).thenReturn(6L);when(db.queryForList(anyString(),any(Object[].class))).thenReturn(List.of());var value=new Pages(db).reusablePolicies("2","5",query);assertEquals(6,value.total());assertEquals(2,value.totalPages());verify(db).queryForList(contains("where t.status='SUCCEEDED' and g.output_dataset_id is not null and (locate(?,d.name)>0 or locate(?,t.id)>0) order by t.created_at desc,t.id desc limit ? offset ?"),eq(query),eq(query),eq(5),eq(5L));
 }
 @Test void invalidPolicySearchNeverQueriesDatabase(){
  var db=mock(JdbcTemplate.class);var pages=new Pages(db);assertThrows(ResponseStatusException.class,()->pages.reusablePolicies("0",null,null));assertThrows(ResponseStatusException.class,()->pages.reusablePolicies(null,"101",null));assertThrows(ResponseStatusException.class,()->pages.reusablePolicies(null,null,"x".repeat(201)));verifyNoInteractions(db);
 }
}
