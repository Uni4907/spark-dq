package cn.sparkdq;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

/** Runs only against a newly created isolated fixture server; never the metadata database. */
class MysqlSnapshotIntegrationTest {
 @TempDir Path root;
 static String url,rootPassword,readerPassword;
 final MysqlSnapshot.Table table=new MysqlSnapshot.Table("business_fixture","sample",List.of(new MysqlSnapshot.Column("id","id"),new MysqlSnapshot.Column("金额","amount"),new MysqlSnapshot.Column("text","text"),new MysqlSnapshot.Column("day","day")));
 final List<String> fields=List.of("id","金额","text","day");
 final MysqlSnapshot.Limits limits=new MysqlSnapshot.Limits(100,20*1024*1024,10);
 @BeforeAll static void fixture()throws Exception {
  url=System.getenv("SNAPSHOT_TEST_URL");Assumptions.assumeTrue(url!=null,"isolated MySQL fixture not requested");
  rootPassword=System.getenv("SNAPSHOT_TEST_ROOT_PASSWORD");readerPassword=System.getenv("SNAPSHOT_TEST_READER_PASSWORD");
  try(var c=DriverManager.getConnection(url,"root",rootPassword);var s=c.createStatement()) {
   s.execute("CREATE DATABASE business_fixture CHARACTER SET utf8mb4");
   try(var p=c.prepareStatement("CREATE USER 'snapshot_reader'@'%' IDENTIFIED BY ?")){p.setString(1,readerPassword);p.execute();}
   s.execute("CREATE TABLE business_fixture.sample(id BIGINT, 金额 DECIMAL(30,10), text TEXT, day DATE) ENGINE=InnoDB");
   s.execute("GRANT SELECT ON business_fixture.sample TO 'snapshot_reader'@'%'");
  }
 }
 Connection reader()throws SQLException{return DriverManager.getConnection(url,"snapshot_reader",readerPassword);}
 @BeforeEach void reset()throws Exception {
  try(var c=DriverManager.getConnection(url,"root",rootPassword);var s=c.createStatement()) {
   s.executeUpdate("DELETE FROM business_fixture.sample");
   s.executeUpdate("INSERT INTO business_fixture.sample VALUES (9007199254740993,9007199254740993.1234567890,'中文\\n换行','2026-10-21'),(2,2.0000000000,'','2026-10-20'),(3,NULL,NULL,NULL),(4,4.0000000000,' ',NULL),(2,2.0000000000,'','2026-10-20')");
  }
 }
 MysqlSnapshot.Proof export(String path,MysqlSnapshot.Limits bound,MysqlSnapshot.Control control)throws Exception {return MysqlSnapshot.export(this::reader,table,fields,bound,control,root.resolve(path));}
 @Test void exactCsvAndDuplicateRows()throws Exception {
  var p=export("snapshot",limits,new MysqlSnapshot.Control());assertEquals(5,p.rows());assertEquals(List.of("id","amount","text","day"),p.columns());
  String expected="\"id\",\"amount\",\"text\",\"day\"\r\n\"9007199254740993\",\"9007199254740993.1234567890\",\"中文\n换行\",\"2026-10-21\"\r\n\"2\",\"2.0000000000\",\"\",\"2026-10-20\"\r\n\"3\",\"\\N\",\"\\N\",\"\\N\"\r\n\"4\",\"4.0000000000\",\" \",\"\\N\"\r\n\"2\",\"2.0000000000\",\"\",\"2026-10-20\"\r\n";
  assertEquals(expected,Files.readString(root.resolve("snapshot/canonical.csv")));assertEquals(p.sha256(),ExecutionInputs.sha(root.resolve("snapshot/canonical.csv")));
  String manifest=Files.readString(root.resolve("snapshot/manifest.json"));assertTrue(manifest.contains("MYSQL_SNAPSHOT"));assertFalse(manifest.contains(readerPassword));
 }
 @Test void failedCredentialsAreSanitized()throws Exception {
  var f=assertThrows(MysqlSnapshot.Failure.class,()->MysqlSnapshot.export(()->DriverManager.getConnection(url,"snapshot_reader","wrong-secret"),table,fields,limits,new MysqlSnapshot.Control(),root.resolve("bad")));
  assertEquals("SNAPSHOT_FAILED",f.code);assertNull(f.getCause());assertFalse(Files.exists(root.resolve("bad")));
 }
 @Test void rowAndByteLimitsLeaveNoSnapshot()throws Exception {
  for(var entry:Map.of("rows",new MysqlSnapshot.Limits(4,1024,10),"bytes",new MysqlSnapshot.Limits(100,50,10)).entrySet()){
   var f=assertThrows(MysqlSnapshot.Failure.class,()->export(entry.getKey(),entry.getValue(),new MysqlSnapshot.Control()));assertEquals(entry.getKey().equals("rows")?"ROW_LIMIT":"BYTE_LIMIT",f.code);assertFalse(Files.exists(root.resolve(entry.getKey())));
  }
  try(var files=Files.list(root)){assertEquals(0,files.count());}
 }
 @Test void readonlyAccountCannotWrite()throws Exception {try(var c=reader();var s=c.createStatement()){assertThrows(SQLException.class,()->s.executeUpdate("DELETE FROM business_fixture.sample"));}}
 @Test void sourceUpdateDoesNotChangeFrozenSnapshot()throws Exception {
  var first=export("old",limits,new MysqlSnapshot.Control());
  try(var c=DriverManager.getConnection(url,"root",rootPassword);var s=c.createStatement()){s.executeUpdate("UPDATE business_fixture.sample SET text='changed'");}
  var second=export("new",limits,new MysqlSnapshot.Control());assertNotEquals(first.sha256(),second.sha256());assertEquals(first.sha256(),ExecutionInputs.sha(root.resolve("old/canonical.csv")));
 }
 @Test void concurrentCommitAfterSnapshotDoesNotMixVersions()throws Exception {
  AtomicBoolean updated=new AtomicBoolean();
  MysqlSnapshot.Connections factory=()->{
   Connection actual=reader();
   return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{Connection.class},(proxy,method,args)->{
    try {
     Object result=method.invoke(actual,args);
     if(method.getName().equals("createStatement")) {
      Statement stmt=(Statement)result;
      return Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{Statement.class},(sp,m,a)->{
       try {
        if(m.getName().equals("executeQuery")&&a[0].toString().contains("FROM `business_fixture`.`sample`")&&!a[0].toString().endsWith("LIMIT 0")&&updated.compareAndSet(false,true)){
         try(var writer=DriverManager.getConnection(url,"root",rootPassword);var change=writer.createStatement()){change.executeUpdate("UPDATE business_fixture.sample SET text='committed-after-snapshot'");}
        }
        return m.invoke(stmt,a);
       }catch(InvocationTargetException e){throw e.getCause();}
      });
     }
     return result;
    }catch(InvocationTargetException e){throw e.getCause();}
   });
  };
  MysqlSnapshot.export(factory,table,fields,limits,new MysqlSnapshot.Control(),root.resolve("consistent"));assertTrue(updated.get());
  String before=Files.readString(root.resolve("consistent/canonical.csv"));assertTrue(before.contains("中文"));assertFalse(before.contains("committed-after-snapshot"));
  export("after",limits,new MysqlSnapshot.Control());assertTrue(Files.readString(root.resolve("after/canonical.csv")).contains("committed-after-snapshot"));
 }
 @Test void cancelAndDeadlineDuringReadPublishNothing()throws Exception {
  for(boolean timeout:List.of(false,true)){
   var control=new MysqlSnapshot.Control();
   MysqlSnapshot.Connections factory=()->{
    var c=reader();
    return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{Connection.class},(proxy,m,args)->{
     try {
      if(m.getName().equals("createStatement")){
       var s=(Statement)m.invoke(c,args);
       return Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{Statement.class},(sp,sm,sa)->{
        try{if(sm.getName().equals("executeQuery")&&!sa[0].toString().endsWith("LIMIT 0")){if(timeout)control.expire();else control.cancel();}return sm.invoke(s,sa);}catch(InvocationTargetException e){throw e.getCause();}
       });
      }
      return m.invoke(c,args);
     }catch(InvocationTargetException e){throw e.getCause();}
    });
   };
   var f=assertThrows(MysqlSnapshot.Failure.class,()->MysqlSnapshot.export(factory,table,fields,limits,control,root.resolve("stopped")));
   assertEquals(timeout?"TIMEOUT":"CANCELLED",f.code);assertFalse(Files.exists(root.resolve("stopped")));
  }
 }
 @Test void actualTimerAndCancellationAbortBlockedJdbc()throws Exception {
  for(boolean timeout:List.of(false,true)){
   try(var writer=DriverManager.getConnection(url,"root",rootPassword);var lock=writer.createStatement()) {
    lock.execute("LOCK TABLES business_fixture.sample WRITE");
    var control=new MysqlSnapshot.Control();var timer=java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
    try {
     if(!timeout)timer.schedule(control::cancel,300,java.util.concurrent.TimeUnit.MILLISECONDS);
     long started=System.nanoTime();
     var f=assertThrows(MysqlSnapshot.Failure.class,()->export("blocked",new MysqlSnapshot.Limits(100,1024,1),control));
     assertEquals(timeout?"TIMEOUT":"CANCELLED",f.code);assertTrue((System.nanoTime()-started)/1_000_000<5000);assertFalse(Files.exists(root.resolve("blocked")));
    }finally{timer.shutdownNow();lock.execute("UNLOCK TABLES");}
   }
  }
 }
 @Test void reservedTextAndUnsupportedEngineFailClosed()throws Exception {
  try(var c=DriverManager.getConnection(url,"root",rootPassword);var s=c.createStatement()) {
   try(var p=c.prepareStatement("UPDATE business_fixture.sample SET text=?")){p.setString(1,"\\N");p.executeUpdate();}
   var text=assertThrows(MysqlSnapshot.Failure.class,()->export("reserved",limits,new MysqlSnapshot.Control()));assertEquals("UNREPRESENTABLE_VALUE",text.code);
   s.execute("CREATE TABLE business_fixture.nontransactional(id BIGINT) ENGINE=MyISAM");s.execute("INSERT INTO business_fixture.nontransactional VALUES (1)");
   s.execute("GRANT SELECT ON business_fixture.nontransactional TO 'snapshot_reader'@'%'");
   var other=new MysqlSnapshot.Table("business_fixture","nontransactional",List.of(new MysqlSnapshot.Column("id","id")));
   var engine=assertThrows(MysqlSnapshot.Failure.class,()->MysqlSnapshot.export(this::reader,other,List.of("id"),limits,new MysqlSnapshot.Control(),root.resolve("myisam")));assertEquals("UNSUPPORTED_TABLE",engine.code);
  }
  try(var files=Files.list(root)){assertEquals(0,files.count());}
 }
 @Test void oversizedTextIsRejectedBeforePublication()throws Exception {
  try(var c=DriverManager.getConnection(url,"root",rootPassword);var s=c.createStatement()) {
   s.execute("ALTER TABLE business_fixture.sample MODIFY text MEDIUMTEXT");
   try(var p=c.prepareStatement("UPDATE business_fixture.sample SET text=?")){p.setString(1,"中".repeat(70000));p.executeUpdate();}
   var f=assertThrows(MysqlSnapshot.Failure.class,()->export("large-cell",limits,new MysqlSnapshot.Control()));assertEquals("UNREPRESENTABLE_VALUE",f.code);
  }
  assertFalse(Files.exists(root.resolve("large-cell")));
 }
}
