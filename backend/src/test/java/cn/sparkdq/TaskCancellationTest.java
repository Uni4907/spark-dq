package cn.sparkdq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.*;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.web.server.ResponseStatusException;
import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class TaskCancellationTest {
 @TempDir Path temp;
 final List<Fixture> fixtures=new ArrayList<>();
 @AfterEach void cleanup(){for(var fixture:fixtures){fixture.releaseAll();fixture.tasks.stop();}}
 Fixture fixture()throws Exception{var fixture=new Fixture(temp.resolve("f"+fixtures.size()));fixtures.add(fixture);return fixture;}
 static void await(CountDownLatch latch)throws Exception{assertTrue(latch.await(5,TimeUnit.SECONDS),"lifecycle checkpoint timed out");}
 static void waitUntil(java.util.function.BooleanSupplier test)throws Exception{long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!test.getAsBoolean()&&System.nanoTime()<end)Thread.sleep(10);assertTrue(test.getAsBoolean());}

 @Test void queuedCancellationFreesCapacityAndOnlyTheTargetIsStopped()throws Exception{
  var f=fixture();var first=f.newProcess();String running=f.create();await(first.launched);
  var queued=new ArrayList<String>();for(int i=0;i<8;i++)queued.add(f.create());
  assertEquals(0,f.tasks.pool.getQueue().remainingCapacity());
  var full=assertThrows(ResponseStatusException.class,f::create);assertEquals(429,full.getStatusCode().value());
  assertEquals("CANCELLED",f.tasks.cancel(queued.get(0)).get("status"));
  assertEquals(1,f.tasks.pool.getQueue().remainingCapacity());
  String replacement=f.create();assertEquals("QUEUED",f.states.get(replacement));
  f.tasks.cancel(queued.get(0));assertTrue(first.isAlive());assertEquals(0,first.kills.get());
  for(int i=1;i<queued.size();i++)f.tasks.cancel(queued.get(i));
  var next=f.newProcess();f.tasks.cancel(running);await(next.launched);
  assertEquals("CANCELLED",f.states.get(running));assertTrue(next.isAlive());assertEquals(0,next.kills.get());
  next.finish(0);waitUntil(()->"SUCCEEDED".equals(f.states.get(replacement)));
  assertEquals(2,f.launchCount.get());assertEquals(2,f.resultCount.get());
 }
 @Test void cancelBetweenCommitAndQueuePublicationStillReleasesTheSlot()throws Exception{
  var f=fixture();var first=f.newProcess();String running=f.create();await(first.launched);
  f.pauseCommitThread="enqueue-race";f.commitPaused=new CountDownLatch(1);f.allowCommitReturn=new CountDownLatch(1);
  var created=new AtomicReference<String>();var creator=new Thread(()->created.set(f.create()),"enqueue-race");creator.start();await(f.commitPaused);
  String queued=f.lastInserted.get();assertFalse(f.tasks.jobs.containsKey(queued));
  var cancelDone=new CountDownLatch(1);var canceller=new Thread(()->{f.tasks.cancel(queued);cancelDone.countDown();});canceller.start();
  waitUntil(()->"CANCELLED".equals(f.states.get(queued)));assertFalse(cancelDone.await(100,TimeUnit.MILLISECONDS));
  f.allowCommitReturn.countDown();creator.join(5000);await(cancelDone);assertEquals(queued,created.get());
  assertEquals(8,f.tasks.pool.getQueue().remainingCapacity());assertFalse(f.tasks.jobs.containsKey(queued));assertTrue(first.isAlive());
  f.tasks.cancel(running);
 }
 @Test void launchingProcessAfterCancelIsImmediatelyStoppedBeforePublishingResults()throws Exception{
  var f=fixture();var process=f.newProcess();f.pauseLaunch=new CountDownLatch(1);f.allowLaunch=new CountDownLatch(1);
  String id=f.create();await(f.pauseLaunch);
  assertEquals("CANCELLING",f.tasks.cancel(id).get("status"));assertNull(f.tasks.jobs.get(id).process);
  f.allowLaunch.countDown();waitUntil(()->"CANCELLED".equals(f.states.get(id)));
  assertFalse(process.isAlive());assertTrue(process.kills.get()>0);assertEquals(0,f.resultCount.get());
 }
 @Test void cancellingDoesNotBecomeCancelledUntilChildAndDescendantsExit()throws Exception{
  var f=fixture();var process=f.newProcess();process.stopOnDestroy=false;
  ProcessHandle child=mock(ProcessHandle.class);AtomicBoolean childAlive=new AtomicBoolean(true);
  when(child.isAlive()).thenAnswer(inv->childAlive.get());when(child.destroyForcibly()).thenReturn(true);process.children=List.of(child);
  String id=f.create();await(process.launched);
  assertEquals("CANCELLING",f.tasks.cancel(id).get("status"));assertEquals("CANCELLING",f.tasks.cancel(id).get("status"));
  assertTrue(process.isAlive());process.finish(137);Thread.sleep(100);assertEquals("CANCELLING",f.states.get(id));
  childAlive.set(false);waitUntil(()->"CANCELLED".equals(f.states.get(id)));assertEquals(0,f.resultCount.get());
  verify(child,atLeastOnce()).destroyForcibly();
 }
 @Test void cleanupTimeoutRetainsProcessAndLaterRetryDoesNotKillNextJob()throws Exception{
  var f=fixture();f.shortCleanupWait=true;var first=f.newProcess();first.stopOnDestroy=false;
  ProcessHandle child=mock(ProcessHandle.class);AtomicBoolean childAlive=new AtomicBoolean(true);
  when(child.isAlive()).thenAnswer(inv->childAlive.get());when(child.destroyForcibly()).thenReturn(true);first.children=List.of(child);
  String cancelled=f.create();await(first.launched);var next=f.newProcess();String replacement=f.create();
  f.tasks.cancel(cancelled);first.finish(137);await(next.launched);
  assertEquals("CANCELLING",f.states.get(cancelled));assertTrue(f.tasks.jobs.containsKey(cancelled));assertTrue(f.tasks.jobs.get(cancelled).runnerFinished);
  assertTrue(next.isAlive());assertEquals(0,next.kills.get());
  childAlive.set(false);f.tasks.reap();waitUntil(()->"CANCELLED".equals(f.states.get(cancelled)));
  assertFalse(f.tasks.jobs.containsKey(cancelled));assertTrue(next.isAlive());assertEquals(0,next.kills.get());
  next.finish(0);waitUntil(()->"SUCCEEDED".equals(f.states.get(replacement)));
 }
 @Test void cancellationWinsBeforeSuccessTransactionAndSkipsGovernanceRegistration()throws Exception{
  var f=fixture();var process=f.newProcess();f.pauseRunnerTransaction=true;f.runnerTransactionPaused=new CountDownLatch(1);f.allowRunnerTransaction=new CountDownLatch(1);
  String id="govern-race";f.states.put(id,"QUEUED");
  var runner=new Thread(()->f.tasks.run(id,f.input.toString(),List.of(Map.of("id","rule","type","MISSING","columns",List.of("name"))),f.sha,new Tasks.GovernanceRequest(false,false,Map.of("name","filled"),Map.of(),2),"csv-v2"),"direct-runner");runner.start();await(process.launched);
  process.finish(0);await(f.runnerTransactionPaused);f.tasks.cancel(id);f.allowRunnerTransaction.countDown();runner.join(5000);
  assertFalse(runner.isAlive());assertEquals("CANCELLED",f.states.get(id));assertEquals(0,f.resultCount.get());assertEquals(0,f.registerCount.get());assertEquals(0,f.versionCount.get());
 }
 @Test void successfulTransactionWinsAndCancelCannotRewriteHistory()throws Exception{
  var f=fixture();var process=f.newProcess();f.pauseResults=new CountDownLatch(1);f.allowResults=new CountDownLatch(1);
  String id=f.create();await(process.launched);process.finish(0);await(f.pauseResults);
  var cancelOutcome=new AtomicReference<Object>();var done=new CountDownLatch(1);
  new Thread(()->{try{cancelOutcome.set(f.tasks.cancel(id));}catch(Exception e){cancelOutcome.set(e);}finally{done.countDown();}}).start();
  assertFalse(done.await(100,TimeUnit.MILLISECONDS));f.allowResults.countDown();await(done);
  assertInstanceOf(ResponseStatusException.class,cancelOutcome.get());assertEquals(409,((ResponseStatusException)cancelOutcome.get()).getStatusCode().value());
  assertEquals("SUCCEEDED",f.states.get(id));assertEquals(2,f.resultCount.get());
 }
 @Test void cancelledIsIdempotentButOtherTerminalStatusesAreRejectedAndUnknownIs404()throws Exception{
  var f=fixture();f.states.put("cancelled","CANCELLED");assertEquals("CANCELLED",f.tasks.cancel("cancelled").get("status"));
  for(String status:List.of("SUCCEEDED","FAILED")){f.states.put("terminal",status);assertEquals(409,assertThrows(ResponseStatusException.class,()->f.tasks.cancel("terminal")).getStatusCode().value());assertEquals(status,f.states.get("terminal"));}
  assertEquals(404,assertThrows(ResponseStatusException.class,()->f.tasks.cancel("absent")).getStatusCode().value());
 }
 @Test void restartDistinguishesExplicitCancellationAndShutdownPreservesIt()throws Exception{
  var f=fixture();f.states.put("cancel","CANCELLING");f.states.put("queue","QUEUED");f.states.put("run","RUNNING");f.states.put("success","SUCCEEDED");f.tasks.recover();
  assertEquals("CANCELLED",f.states.get("cancel"));assertEquals("FAILED",f.states.get("queue"));assertEquals("FAILED",f.states.get("run"));assertEquals("SUCCEEDED",f.states.get("success"));
  var process=f.newProcess();process.stopOnDestroy=false;String id=f.create();await(process.launched);f.tasks.cancel(id);
  var stop=new Thread(f.tasks::stop);stop.start();process.finish(137);stop.join(5000);assertFalse(stop.isAlive());assertEquals("CANCELLED",f.states.get(id));assertTrue(f.tasks.jobs.isEmpty());
 }
 @Test void cancelledReportHasNoSuccessOutputOrComparison(){
  Tasks tasks=mock(Tasks.class);var cancelled=new HashMap<String,Object>();cancelled.put("id","cancel");cancelled.put("status","CANCELLED");cancelled.put("rules",List.of());cancelled.put("snapshot",Map.of("parent_task_id","parent","semantics_version","csv-v2"));
  when(tasks.detail("cancel")).thenReturn(cancelled);when(tasks.detail("parent")).thenReturn(Map.of("id","parent","status","SUCCEEDED"));
  var report=new Reports(tasks).report("cancel");assertSame(cancelled,report.get("task"));assertFalse(report.containsKey("outputDataset"));assertFalse(report.containsKey("comparison"));
 }

 final class Fixture {
  final Path root,input;final String sha;final JdbcTemplate db=mock(JdbcTemplate.class);final PlatformTransactionManager manager=mock(PlatformTransactionManager.class);
  final Map<String,String> states=new ConcurrentHashMap<>();final ReentrantLock transaction=new ReentrantLock();
  final AtomicInteger resultCount=new AtomicInteger(),versionCount=new AtomicInteger(),registerCount=new AtomicInteger(),launchCount=new AtomicInteger();
  final AtomicReference<String> lastInserted=new AtomicReference<>();final BlockingQueue<FakeProcess> processes=new LinkedBlockingQueue<>();final List<FakeProcess> allProcesses=new ArrayList<>();
  volatile String pauseCommitThread;volatile boolean pauseRunnerTransaction,shortCleanupWait;
  volatile CountDownLatch commitPaused,allowCommitReturn,pauseLaunch,allowLaunch,runnerTransactionPaused,allowRunnerTransaction,pauseResults,allowResults;
  final Tasks tasks;
  Fixture(Path root)throws Exception{
   this.root=root;Files.createDirectories(root);input=root.resolve("input.csv");Files.writeString(input,"id,name\n1,A\n");sha=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(input)));
   when(manager.getTransaction(any())).thenAnswer(inv->{
    if(pauseRunnerTransaction&&Thread.currentThread().getName().equals("direct-runner")){runnerTransactionPaused.countDown();assertTrue(allowRunnerTransaction.await(5,TimeUnit.SECONDS));}
    transaction.lock();return new SimpleTransactionStatus();
   });
   doAnswer(inv->{transaction.unlock();if(Thread.currentThread().getName().equals(pauseCommitThread)){commitPaused.countDown();assertTrue(allowCommitReturn.await(5,TimeUnit.SECONDS));}return null;}).when(manager).commit(any());
   doAnswer(inv->{transaction.unlock();return null;}).when(manager).rollback(any());
   when(db.queryForList(anyString(),any(Object[].class))).thenAnswer(inv->{String sql=inv.getArgument(0);if(sql.startsWith("select status")){String status=states.get(inv.getArgument(1));return status==null?List.of():List.of(Map.of("status",status));}return List.of();});
   when(db.update(anyString())).thenAnswer(inv->{
    String sql=inv.getArgument(0);
    states.replaceAll((id,status)->sql.contains("status='CANCELLING'")?(status.equals("CANCELLING")?"CANCELLED":status):(Set.of("QUEUED","RUNNING").contains(status)?"FAILED":status));return 1;
   });
   when(db.update(anyString(),any(Object[].class))).thenAnswer(inv->{
    transaction.lock();try{
     String sql=inv.getArgument(0);Object[] args=inv.getArguments();
     if(sql.startsWith("insert into tasks")){String id=args[1].toString();states.put(id,"QUEUED");lastInserted.set(id);return 1;}
     if(sql.startsWith("insert into results")){if(pauseResults!=null){pauseResults.countDown();assertTrue(allowResults.await(5,TimeUnit.SECONDS));}resultCount.incrementAndGet();return 1;}
     if(sql.startsWith("insert into dataset_versions")){versionCount.incrementAndGet();return 1;}
     if(!sql.startsWith("update tasks"))return 1;
     if(args.length==1){
      states.replaceAll((id,status)->sql.contains("status='CANCELLING'")?(status.equals("CANCELLING")?"CANCELLED":status):(Set.of("QUEUED","RUNNING").contains(status)?"FAILED":status));return 1;
     }
     String id=args[args.length-1].toString(),current=states.get(id);
     if(sql.contains("and status='QUEUED'")&&!"QUEUED".equals(current)||sql.contains("and status='RUNNING'")&&!"RUNNING".equals(current)||sql.contains("and status='CANCELLING'")&&!"CANCELLING".equals(current))return 0;
     for(String state:List.of("CANCELLED","CANCELLING","SUCCEEDED","FAILED","RUNNING"))if(sql.startsWith("update tasks set status='"+state+"'")){states.put(id,state);return 1;}
     return 1;
    }finally{transaction.unlock();}
   });
   Datasets data=new Datasets(db,new ObjectMapper()){
    @Override public Map<String,Object> one(String id){return new HashMap<>(Map.of("id",id,"columns",List.of("id","name"),"file_path",input.toString(),"sha256",sha));}
    @Override Map<String,Object> register(String name,InputStream stream,boolean empty){registerCount.incrementAndGet();return Map.of("id","version","row_count",1L);}
   };
   tasks=new Tasks(data,manager){
    @Override public Map<String,Object> detail(String id){return Map.of("id",id,"status",states.get(id));}
    @Override Path taskDirectory(String id){return root.resolve(id);}
    @Override long terminationWaitMillis(){return shortCleanupWait?100:super.terminationWaitMillis();}
    @Override Process launchProcess(List<String> command,Path log)throws Exception{
     FakeProcess process=processes.poll(5,TimeUnit.SECONDS);assertNotNull(process);launchCount.incrementAndGet();
     var request=ds.json.readTree(log.getParent().resolve("request.json").toFile());var results=new ArrayList<Object>();
     for(var rule:request.path("rules"))results.add(Map.of("ruleId",rule.path("id").asText(),"metrics",Map.of(),"anomalyPath","unused"));
     ds.json.writeValue(log.getParent().resolve("result.json").toFile(),Map.of("totalRows",1,"sparkVersion","test","results",results,"governance",Map.of("inputRows",1)));
     if(request.has("governance")){Files.createDirectories(log.getParent().resolve("governed"));Files.writeString(log.getParent().resolve("governed/part-test.csv"),"id,name\n1,A\n");}
     process.launched.countDown();if(pauseLaunch!=null){pauseLaunch.countDown();assertTrue(allowLaunch.await(5,TimeUnit.SECONDS));}return process;
    }
   };
  }
  FakeProcess newProcess(){var process=new FakeProcess();processes.add(process);allProcesses.add(process);return process;}
  String create(){return tasks.create(new Tasks.Request("dataset",List.of("name"),List.of("id"))).get("id").toString();}
  void releaseAll(){for(var latch:Arrays.asList(allowCommitReturn,allowLaunch,allowRunnerTransaction,allowResults))if(latch!=null)latch.countDown();for(var process:allProcesses)process.finish(137);}
 }
 static final class FakeProcess extends Process {
  final CountDownLatch launched=new CountDownLatch(1),exited=new CountDownLatch(1);final AtomicInteger kills=new AtomicInteger();volatile boolean alive=true,stopOnDestroy=true;volatile int code;volatile List<ProcessHandle> children=List.of();
  void finish(int code){this.code=code;alive=false;exited.countDown();}
  @Override public OutputStream getOutputStream(){return OutputStream.nullOutputStream();}
  @Override public InputStream getInputStream(){return InputStream.nullInputStream();}
  @Override public InputStream getErrorStream(){return InputStream.nullInputStream();}
  @Override public int waitFor()throws InterruptedException{exited.await();return code;}
  @Override public boolean waitFor(long timeout,TimeUnit unit)throws InterruptedException{return exited.await(timeout,unit);}
  @Override public int exitValue(){if(alive)throw new IllegalThreadStateException();return code;}
  @Override public void destroy(){destroyForcibly();}
  @Override public Process destroyForcibly(){kills.incrementAndGet();if(stopOnDestroy)finish(137);return this;}
  @Override public boolean isAlive(){return alive;}
  @Override public Stream<ProcessHandle> descendants(){return children.stream();}
 }
}
