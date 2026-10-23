package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.*;
import org.springframework.web.server.ResponseStatusException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MaintenanceTest {
 @TempDir Path temporary;
 JdbcTemplate db;Maintenance maintenance;MaintenanceFilter filter;
 @BeforeEach void setup()throws Exception {
  db=mock(JdbcTemplate.class);when(db.queryForObject(anyString(),eq(Long.class))).thenReturn(0L);
  maintenance=new Maintenance(db,temporary.resolve("owner"),temporary.resolve("key"));
  filter=new MaintenanceFilter(maintenance,new ObjectMapper());
 }
 String owner(){return UUID.randomUUID().toString();}
 @Test void secretRequiredAndPrivate()throws Exception {
  assertThrows(ResponseStatusException.class,()->maintenance.authorize(null));
  assertThrows(ResponseStatusException.class,()->maintenance.authorize("wrong"));
  assertDoesNotThrow(()->maintenance.authorize(maintenance.key));
  assertEquals("rw-------",Files.getPosixFilePermissions(temporary.resolve("key")).stream().collect(
   java.util.stream.Collectors.collectingAndThen(java.util.stream.Collectors.toSet(),java.nio.file.attribute.PosixFilePermissions::toString)));
  assertFalse(maintenance.state().containsValue(maintenance.key));
 }
 @Test void busyRefusesWithoutClosingAdmission() {
  when(db.queryForObject(anyString(),eq(Long.class))).thenReturn(1L);
  var error=assertThrows(ResponseStatusException.class,()->maintenance.enter(owner()));
  assertEquals(409,error.getStatusCode().value());assertFalse(maintenance.enabled());
  assertFalse(Files.exists(maintenance.marker));
 }
 @Test void ownersAreIdempotentAndCannotReleaseAnotherSession()throws Exception {
  String first=owner();maintenance.enter(first);maintenance.enter(first);
  verify(db,times(1)).queryForObject(anyString(),eq(Long.class));
  assertThrows(ResponseStatusException.class,()->maintenance.enter(owner()));
  assertThrows(ResponseStatusException.class,()->maintenance.leave(owner()));
  assertTrue(maintenance.enabled());maintenance.leave(first);maintenance.leave(first);
  assertFalse(maintenance.enabled());assertFalse(Files.exists(maintenance.marker));
 }
 @Test void restartRetainsGateButRotatesSecret()throws Exception {
  String session=owner();maintenance.enter(session);
  var restarted=new Maintenance(db,maintenance.marker,temporary.resolve("key"));
  assertTrue(restarted.enabled());assertEquals(session,restarted.owner);
  assertThrows(ResponseStatusException.class,()->restarted.authorize(maintenance.key));
  restarted.leave(session);assertFalse(restarted.enabled());
 }
 @Test void invalidOrUnreadableMarkerFailsClosed()throws Exception {
  Files.writeString(maintenance.marker,"not-an-owner");
  assertThrows(ResponseStatusException.class,()->new Maintenance(db,maintenance.marker,temporary.resolve("key")));
 }
 @Test void gateBlocksEveryMutationButRetainsReads()throws Exception {
  maintenance.enter(owner());
  for(String method:List.of("POST","PUT","PATCH","DELETE"))
   for(String path:List.of("/api/tasks","/api/tasks/id/rerun","/api/tasks/id/govern","/api/tasks/id/cancel","/api/datasets","/api/rule-templates/id")) {
    var request=new MockHttpServletRequest(method,path);var response=new MockHttpServletResponse();var reached=new AtomicBoolean();
    filter.doFilter(request,response,(a,b)->reached.set(true));
    assertEquals(503,response.getStatus());assertFalse(reached.get());
    assertEquals("30",response.getHeader("Retry-After"));
   }
  for(String method:List.of("GET","HEAD","OPTIONS")) {
   var response=new MockHttpServletResponse();
   filter.doFilter(new MockHttpServletRequest(method,"/api/tasks"),response,(a,b)->((jakarta.servlet.http.HttpServletResponse)b).setStatus(204));
   assertEquals(204,response.getStatus());
  }
 }
 @Test void maintenanceWaitsForInflightWriteAndBlocksLaterWrites()throws Exception {
  var pool=Executors.newFixedThreadPool(3);var started=new CountDownLatch(1);var release=new CountDownLatch(1);
  try {
   var first=pool.submit(()->{
    filter.doFilter(new MockHttpServletRequest("POST","/api/datasets"),new MockHttpServletResponse(),(a,b)->{
     started.countDown();
     try {if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("Test write timeout");}
     catch(InterruptedException e){Thread.currentThread().interrupt();throw new java.io.IOException(e);}
    });return true;
   });
   assertTrue(started.await(2,TimeUnit.SECONDS));
   var entry=pool.submit(()->maintenance.enter(owner()));
   long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);
   while(!maintenance.gate.hasQueuedThreads()&&System.nanoTime()<end)Thread.yield();
   assertTrue(maintenance.gate.hasQueuedThreads());assertFalse(entry.isDone());
   verify(db,never()).queryForObject(anyString(),eq(Long.class));
   var later=pool.submit(()->{
    var response=new MockHttpServletResponse();
    filter.doFilter(new MockHttpServletRequest("POST","/api/tasks"),response,(a,b)->fail("Later write entered gate"));
    return response.getStatus();
   });
   assertEquals(503,later.get(2,TimeUnit.SECONDS));
   release.countDown();assertTrue(first.get(2,TimeUnit.SECONDS));
   assertEquals(true,entry.get(2,TimeUnit.SECONDS).get("maintenance"));
  } finally {release.countDown();pool.shutdownNow();assertTrue(pool.awaitTermination(5,TimeUnit.SECONDS));}
 }
 @Test void authenticatedControlCanReleaseWhileGateIsClosed()throws Exception {
  String session=owner();maintenance.enter(session);
  var api=new MaintenanceApi(maintenance);
  assertThrows(ResponseStatusException.class,()->api.leave(null,new MaintenanceApi.Request(session)));
  assertTrue(maintenance.enabled());
  var response=new MockHttpServletResponse();
  filter.doFilter(new MockHttpServletRequest("POST","/api/internal/maintenance/leave"),response,(a,b)->{
   assertEquals(false,api.leave(maintenance.key,new MaintenanceApi.Request(session)).get("maintenance"));
  });
  assertFalse(maintenance.enabled());
 }
}
