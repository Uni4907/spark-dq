package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.*;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.web.server.ResponseStatusException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class NativeImportsTest {
 JdbcTemplate db;Tasks tasks;NativeImports imports;
 @BeforeEach void setup(){db=mock(JdbcTemplate.class);var ds=new Datasets(db,new ObjectMapper());var manager=mock(PlatformTransactionManager.class);when(manager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());tasks=new Tasks(ds,manager);imports=new NativeImports(ds,tasks,manager);}
 @AfterEach void stop(){imports.stop();tasks.stop();}
 @Test void exactUuidKeyAndFilenameRequired(){for(String key:Arrays.asList(null,"", "1-1-1-1-1",UUID.randomUUID().toString().toUpperCase()))assertThrows(ResponseStatusException.class,()->NativeImports.key(key));String key=UUID.randomUUID().toString();assertEquals(key,NativeImports.key(key));assertEquals("a.parquet",NativeImports.filename("C:\\fakepath\\a.parquet"));assertThrows(ResponseStatusException.class,()->NativeImports.filename("a.csv"));}
 @Test void queuedCancellationRemovesSharedQueueEntry(){String id=UUID.randomUUID().toString();when(db.queryForList("select status from dataset_imports where id=? and source_kind='PARQUET_NATIVE' for update",id)).thenReturn(List.of(Map.of("status","QUEUED")));when(db.queryForList("select * from dataset_imports where id=? and source_kind='PARQUET_NATIVE'",id)).thenReturn(List.of(Map.of("id",id,"status","CANCELLED")));var job=imports.new ImportJob(id,"0".repeat(64),12);imports.jobs.put(id,job);tasks.pool.getQueue().add(job);assertEquals("CANCELLED",imports.cancel(id).get("status"));assertTrue(tasks.pool.getQueue().isEmpty());assertFalse(imports.jobs.containsKey(id));assertTrue(job.owner.cancelled);}
 @Test void successCannotBeCancelled(){String id=UUID.randomUUID().toString();when(db.queryForList("select status from dataset_imports where id=? and source_kind='PARQUET_NATIVE' for update",id)).thenReturn(List.of(Map.of("status","SUCCEEDED")));assertEquals(409,assertThrows(ResponseStatusException.class,()->imports.cancel(id)).getStatusCode().value());}
 @Test void restartOnlyRecoversUnfinishedStates(){imports.recover();verify(db).update(contains("where source_kind='PARQUET_NATIVE' and status='CANCELLING'"));verify(db).update(contains("where source_kind='PARQUET_NATIVE' and status in ('QUEUED','RUNNING')"));}
 @Test void expiredWorkerBudgetDoesNotLaunchProcess()throws Exception{var job=tasks.new Job("test","unused",List.of(),"sha",null,"csv-v2");assertThrows(IllegalStateException.class,()->tasks.runWorker(job,"unused",java.nio.file.Path.of("unused"),java.nio.file.Path.of("unused"),System.nanoTime()-TimeUnit.SECONDS.toNanos(301)));assertNull(job.process);}
}
