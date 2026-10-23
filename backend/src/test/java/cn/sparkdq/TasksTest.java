package cn.sparkdq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.server.ResponseStatusException;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class TasksTest {
 @TempDir Path temp;
 @Test void changedInputFailsBeforeLaunchingSparkAndPersistsReason() throws Exception {
  JdbcTemplate db=mock(JdbcTemplate.class);
  when(db.update(eq("update tasks set status='RUNNING',started_at=CURRENT_TIMESTAMP(3) where id=? and status='QUEUED'"),eq("checksum-test"))).thenReturn(1);
  Tasks tasks=new Tasks(new Datasets(db,new ObjectMapper()),mock(PlatformTransactionManager.class));
  Path input=temp.resolve("input.csv");Files.writeString(input,"id,name\n1,A\n");
  tasks.run("checksum-test",input.toString(),List.of(),"0".repeat(64));
  ArgumentCaptor<String> reason=ArgumentCaptor.forClass(String.class);
  verify(db).update(eq("update tasks set status='FAILED',error=?,duration_ms=?,finished_at=CURRENT_TIMESTAMP(3) where id=? and status='RUNNING'"),reason.capture(),anyLong(),eq("checksum-test"));
  assertTrue(reason.getValue().contains("SHA-256"));
  assertTrue(tasks.jobs.isEmpty());
  verify(db,never()).update(startsWith("insert into results"),any(Object[].class));
 }
 @Test void stoppedSchedulerRejectsBeforeCreatingDurableTask() {
  JdbcTemplate db=mock(JdbcTemplate.class);
  when(db.queryForList("select * from datasets where id=?","dataset")).thenReturn(List.of(new HashMap<>(Map.of("id","dataset","columns_json","[\"id\",\"name\"]","preview_json","[]"))));
  Tasks tasks=new Tasks(new Datasets(db,new ObjectMapper()),mock(PlatformTransactionManager.class));
  tasks.pool.shutdown();
  var failure=assertThrows(ResponseStatusException.class,()->tasks.create(new Tasks.Request("dataset",List.of("name"),List.of("id"))));
  assertEquals(503,failure.getStatusCode().value());
  verify(db,never()).update(anyString(),any(Object[].class));
 }
}
