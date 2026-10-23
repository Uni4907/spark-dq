package cn.sparkdq;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class PagesTest {
 @Test void invalidRequestsNeverReachDatabase(){
  var db=mock(JdbcTemplate.class);var pages=new Pages(db);
  for(String page:List.of("","0","-1","+1","1.0"," 1","2147483648","x"))
   assertEquals(400,assertThrows(ResponseStatusException.class,()->pages.tasks(page,"20","ALL")).getStatusCode().value());
  for(String size:List.of("0","101","-2","1.5",""))
   assertThrows(ResponseStatusException.class,()->pages.datasets("1",size,null));
  for(String state:List.of("","running","CANCELLED or 1=1","UNKNOWN"))
   assertThrows(ResponseStatusException.class,()->pages.tasks("1","20",state));
  assertThrows(ResponseStatusException.class,()->pages.datasets("1","20","x".repeat(201)));
  verifyNoInteractions(db);
 }
 @Test void largePageOffsetDoesNotOverflowInteger(){
  var db=mock(JdbcTemplate.class);var pages=new Pages(db);
  when(db.queryForObject("select count(*) from tasks t",Long.class,new Object[0])).thenReturn(2L);
  when(db.queryForList(anyString(),any(Object[].class))).thenReturn(List.of());
  var page=pages.tasks("2147483647","100","ALL");
  assertEquals(2147483647,page.page());assertEquals(1,page.totalPages());
  verify(db).queryForList(anyString(),eq(100),eq(214748364600L));
 }
 @Test void exactStatusFilterUsesPreparedValuesAndStableOrdering(){
  var db=mock(JdbcTemplate.class);var pages=new Pages(db);
  when(db.queryForObject("select count(*) from tasks t where t.status=?",Long.class,"CANCELLED")).thenReturn(5L);
  var items=List.<Map<String,Object>>of(Map.of("id","cancelled"));
  when(db.queryForList(anyString(),any(Object[].class))).thenReturn(items);
  var page=pages.tasks("2","2","CANCELLED");
  assertEquals(5,page.total());assertEquals(3,page.totalPages());assertSame(items,page.items());
  verify(db).queryForList(contains("where t.status=? order by t.created_at desc,t.id desc limit ? offset ?"),eq("CANCELLED"),eq(2),eq(2L));
 }
 @Test void literalSearchDoesNotExpandWildcardsOrInjectSql(){
  var db=mock(JdbcTemplate.class);var pages=new Pages(db);
  String query="a%_' OR 1=1";
  when(db.queryForObject("select count(*) from datasets t where locate(?,t.name)>0",Long.class,query)).thenReturn(1L);
  when(db.queryForList(anyString(),any(Object[].class))).thenReturn(List.of(Map.of("id","literal")));
  var page=pages.datasets(null,null,"  "+query+"  ");
  assertEquals(1,page.total());assertEquals(1,page.page());assertEquals(20,page.pageSize());
  verify(db).queryForList(contains("where locate(?,t.name)>0 order by t.created_at desc,t.id desc limit ? offset ?"),eq(query),eq(20),eq(0L));
 }
 @Test void emptyResultsKeepRequestedPageAndHaveZeroPages(){
  var db=mock(JdbcTemplate.class);var pages=new Pages(db);
  when(db.queryForObject("select count(*) from datasets t",Long.class,new Object[0])).thenReturn(0L);
  when(db.queryForList(anyString(),any(Object[].class))).thenReturn(List.of());
  var page=pages.datasets("3","1","  ");
  assertEquals(0,page.total());assertEquals(0,page.totalPages());assertEquals(3,page.page());assertTrue(page.items().isEmpty());
 }
}
