package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.sql.Connection;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.*;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.web.server.ResponseStatusException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MysqlImportsTest {
 @TempDir Path root;JdbcTemplate db;Tasks tasks;MysqlImports imports;Datasets ds;MysqlSources.Source source;
 @BeforeEach void setup(){
  db=mock(JdbcTemplate.class);ds=new Datasets(db,new ObjectMapper());var manager=mock(PlatformTransactionManager.class);when(manager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());tasks=new Tasks(ds,manager);
  source=new MysqlSources.Source("business","business-mysql",3306,"reader","DISABLED",root.resolve("password"),new MysqlSnapshot.Table("business","sample",List.of(new MysqlSnapshot.Column("id","id"))),new MysqlSnapshot.Limits(100,1024,5));
  imports=new MysqlImports(ds,tasks,new MysqlSources(Map.of("business",source)),manager);
 }
 @AfterEach void close(){imports.stop();tasks.stop();}
 void state(String id,String state,String after){when(db.queryForList("select * from dataset_imports where id=? and source_kind='MYSQL_SNAPSHOT' for update",id)).thenReturn(List.of(Map.of("status",state)));when(db.queryForList("select * from dataset_imports where id=? and source_kind='MYSQL_SNAPSHOT'",id)).thenReturn(List.of(Map.of("id",id,"status",after)));}
 @Test void queuedCancellationRemovesJobAndSignalsStopped(){String id=UUID.randomUUID().toString();state(id,"QUEUED","CANCELLED");var job=imports.new Job(id,source,List.of("id"));imports.jobs.put(id,job);tasks.pool.getQueue().add(job);assertEquals("CANCELLED",imports.cancel(id).get("status"));assertFalse(imports.jobs.containsKey(id));assertTrue(tasks.pool.getQueue().isEmpty());assertEquals(0,job.stopped.getCount());}
 @Test void runningCancellationAbortsConnectionBeforeTerminalState()throws Exception {String id=UUID.randomUUID().toString();state(id,"RUNNING","CANCELLING");var job=imports.new Job(id,source,List.of("id"));Connection c=mock(Connection.class);job.control.attach(c);imports.jobs.put(id,job);assertEquals("CANCELLING",imports.cancel(id).get("status"));verify(c).abort(any());verify(db,never()).update(contains("set status='CANCELLED'"),any(Object[].class));imports.jobs.remove(id);}
 @Test void successCannotBeCancelled(){String id=UUID.randomUUID().toString();state(id,"SUCCEEDED","SUCCEEDED");assertEquals(409,assertThrows(ResponseStatusException.class,()->imports.cancel(id)).getStatusCode().value());}
 @Test void restartRecoveryIsRestrictedToMysqlKind(){imports.recover();verify(db).update(contains("source_kind='MYSQL_SNAPSHOT' and status='CANCELLING'"));verify(db).update(contains("source_kind='MYSQL_SNAPSHOT' and status in ('QUEUED','RUNNING')"));}
 @Test void idempotencyDoesNotReadSourceAgain()throws Exception {String key=UUID.randomUUID().toString();var request=new MysqlImports.Request("business",List.of("id"));var existing=Map.<String,Object>of("id","existing","source_kind","MYSQL_SNAPSHOT","source_request_json",MysqlImports.requestJson(ds,request),"status","SUCCEEDED");when(db.queryForList("select * from dataset_imports where request_key=?",key)).thenReturn(List.of(existing));assertEquals(existing,imports.submit(request,key));assertTrue(imports.jobs.isEmpty());assertTrue(tasks.pool.getQueue().isEmpty());}
 @Test void crossKindKeysAndUnlistedFieldsAreRejected(){String key=UUID.randomUUID().toString();when(db.queryForList("select * from dataset_imports where request_key=?",key)).thenReturn(List.of(Map.of("source_kind","PARQUET_NATIVE")));assertEquals(409,assertThrows(ResponseStatusException.class,()->imports.submit(new MysqlImports.Request("business",List.of("id")),key)).getStatusCode().value());assertThrows(IllegalArgumentException.class,()->imports.submit(new MysqlImports.Request("business",List.of("secret")),UUID.randomUUID().toString()));}
}
