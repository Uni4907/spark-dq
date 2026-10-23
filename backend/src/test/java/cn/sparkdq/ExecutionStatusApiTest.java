package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
class ExecutionStatusApiTest {
 Tasks tasks;
 @BeforeEach void setup(){tasks=new Tasks(new Datasets(mock(JdbcTemplate.class),new ObjectMapper()),mock(PlatformTransactionManager.class));tasks.cleanup.shutdownNow();}
 @AfterEach void stop(){tasks.stop();}
 @Test void recoveryPendingClosesAdmissionWithoutLeakingTaskIdentity(){tasks.recoveryPending.add("private-task");var result=new ExecutionStatusApi(tasks).status();assertEquals(false,result.get("acceptingTasks"));assertEquals(1,result.get("cleanupPending"));assertFalse(result.toString().contains("private-task"));}
 @Test void stoppedExecutorDoesNotAdvertiseAdmission(){tasks.pool.shutdown();assertEquals(false,new ExecutionStatusApi(tasks).status().get("acceptingTasks"));}
 @Test void idleInstanceAcceptsTasks(){var result=new ExecutionStatusApi(tasks).status();assertEquals("local",result.get("mode"));assertEquals(true,result.get("acceptingTasks"));assertEquals(0,result.get("cleanupPending"));}
}
