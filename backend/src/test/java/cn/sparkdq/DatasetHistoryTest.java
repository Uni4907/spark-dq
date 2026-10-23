package cn.sparkdq;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
class DatasetHistoryTest {
 @Test void invalidPagingStatusAndIdentityRejectBeforeDatabase(){
  var db=mock(JdbcTemplate.class);var pages=new Pages(db);
  for(String id:Arrays.asList(null,""," ","x".repeat(37)))assertThrows(ResponseStatusException.class,()->pages.datasetTasks(id,null,null,null));
  assertThrows(ResponseStatusException.class,()->pages.datasetTasks("data","0","5","ALL"));assertThrows(ResponseStatusException.class,()->pages.datasetTasks("data","1","101","ALL"));assertThrows(ResponseStatusException.class,()->pages.datasetTasks("data",null,null,"SUCCEEDED OR 1=1"));verifyNoInteractions(db);
 }
 @Test void unknownDatasetIs404RatherThanUnrelatedGlobalTasks(){
  var db=mock(JdbcTemplate.class);when(db.queryForObject("select count(*) from datasets where id=?",Long.class,"missing")).thenReturn(0L);
  assertEquals(404,assertThrows(ResponseStatusException.class,()->new Pages(db).datasetTasks("missing",null,null,null)).getStatusCode().value());verify(db,never()).queryForList(anyString(),any(Object[].class));
 }
 @Test void inputAndGeneratedVersionAreIncludedOnceWithBoundedStablePage(){
  var db=mock(JdbcTemplate.class);when(db.queryForObject("select count(*) from datasets where id=?",Long.class,"data")).thenReturn(1L);when(db.queryForObject(contains("left join governance_runs"),eq(Long.class),eq("data"),eq("data"))).thenReturn(6L);
  var items=List.<Map<String,Object>>of(Map.of("id","govern","dataset_id","parent","output_dataset_id","data"));when(db.queryForList(anyString(),any(Object[].class))).thenReturn(items);
  var page=new Pages(db).datasetTasks("data","2","5",null);assertSame(items,page.items());assertEquals(6,page.total());assertEquals(2,page.totalPages());
  verify(db).queryForList(contains("where (t.dataset_id=? or g.output_dataset_id=?) order by t.created_at desc,t.id desc limit ? offset ?"),eq("data"),eq("data"),eq(5),eq(5L));
 }
 @Test void statusAppliesToBothSidesOfDatasetRelation(){
  var db=mock(JdbcTemplate.class);String id="a' OR 1=1";when(db.queryForObject("select count(*) from datasets where id=?",Long.class,id)).thenReturn(1L);when(db.queryForObject(anyString(),eq(Long.class),eq(id),eq(id),eq("FAILED"))).thenReturn(1L);when(db.queryForList(anyString(),any(Object[].class))).thenReturn(List.of());
  assertEquals(1,new Pages(db).datasetTasks(id,"1","5","FAILED").total());verify(db).queryForList(contains("where (t.dataset_id=? or g.output_dataset_id=?) and t.status=? order by"),eq(id),eq(id),eq("FAILED"),eq(5),eq(0L));
 }
}
