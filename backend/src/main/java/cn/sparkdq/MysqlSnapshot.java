package cn.sparkdq;

import org.apache.commons.csv.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;

/** T1-A: single-connection snapshot primitive. Not yet a product import endpoint. */
final class MysqlSnapshot {
 static final long MAX_BYTES=20L*1024*1024;
 record Column(String source,String target) {
  Column { identifier(source); if(target==null||!target.matches("[A-Za-z][A-Za-z0-9_]{0,63}"))throw new IllegalArgumentException("Invalid mapped column"); }
 }
 record Table(String schema,String name,List<Column> allowed) {
  Table { identifier(schema);identifier(name);allowed=List.copyOf(allowed);
   if(allowed.isEmpty()||allowed.size()>100||allowed.stream().map(Column::source).distinct().count()!=allowed.size()||allowed.stream().map(Column::target).distinct().count()!=allowed.size())throw new IllegalArgumentException("Invalid allowlist"); }
 }
 record Limits(long rows,long bytes,int seconds) {
  Limits { if(rows<1||rows>1_000_000||bytes<1||bytes>MAX_BYTES||seconds<1||seconds>300)throw new IllegalArgumentException("Invalid snapshot limits"); }
 }
 record Proof(long rows,long bytes,String sha256,List<String> columns,List<String> types) {}
 @FunctionalInterface interface Connections { Connection open() throws SQLException; }
 static final class Failure extends Exception {
  final String code;
  Failure(String code){super(code);this.code=code;}
 }
 /** Caller owns the control; cancel only signals until JDBC and file cleanup complete. */
 static final class Control {
  private boolean cancelled,expired,finished;private Connection connection;
  synchronized void attach(Connection c)throws Failure {connection=c;check();}
  synchronized void cancel(){if(!finished){cancelled=true;abort();}}
  synchronized void expire(){if(!finished){expired=true;abort();}}
  private void abort(){if(connection!=null)try{connection.abort(Runnable::run);}catch(SQLException ignored){}}
  synchronized void check()throws Failure {if(cancelled)throw new Failure("CANCELLED");if(expired)throw new Failure("TIMEOUT");}
  synchronized void finish()throws Failure {check();finished=true;connection=null;}
 }
 static void identifier(String name){if(name==null||!name.matches("[\\p{L}_][\\p{L}\\p{N}_]{0,63}"))throw new IllegalArgumentException("Invalid source identifier");}
 static String quoted(String name){identifier(name);return "`"+name+"`";}
 static List<Column> select(Table table,List<String> selected){
  if(selected==null||selected.isEmpty()||selected.size()>100||new HashSet<>(selected).size()!=selected.size())throw new IllegalArgumentException("Invalid selection");
  return selected.stream().map(name->table.allowed().stream().filter(c->c.source().equals(name)).findFirst().orElseThrow(()->new IllegalArgumentException("Column outside allowlist"))).toList();
 }
 static String sql(Table table,List<Column> columns,long rows){return "SELECT "+String.join(",",columns.stream().map(c->quoted(c.source())).toList())+" FROM "+quoted(table.schema())+"."+quoted(table.name())+" LIMIT "+rows;}
 static String boundedSql(Table table,List<Column> columns,List<String> types,long rows){
  var expressions=new ArrayList<String>();for(int i=0;i<columns.size();i++){
   String expression=quoted(columns.get(i).source()),type=types.get(i);
   // Fetch one character beyond the contract, then fail; never publish a truncated value.
   if(type.equals("VARCHAR")||type.endsWith("TEXT"))expression="LEFT("+expression+",65537)";
   expressions.add(expression);
  }
  return "SELECT "+String.join(",",expressions)+" FROM "+quoted(table.schema())+"."+quoted(table.name())+" LIMIT "+rows;
 }
 static String type(ResultSetMetaData metadata,int i)throws SQLException,Failure {
  String name=metadata.getColumnTypeName(i).toUpperCase(Locale.ROOT);
  String base=name.replace(" UNSIGNED","");
  if(!Set.of("TINYINT","SMALLINT","MEDIUMINT","INT","INTEGER","BIGINT","DECIMAL","NUMERIC","DATE","VARCHAR","TINYTEXT","TEXT","MEDIUMTEXT","LONGTEXT").contains(base))throw new Failure("UNSUPPORTED_TYPE");
  return name;
 }
 static String value(ResultSet rows,int i,String type)throws SQLException,Failure {
  String base=type.replace(" UNSIGNED","");String value;
  if(base.equals("DECIMAL")||base.equals("NUMERIC")){var n=rows.getBigDecimal(i);value=n==null?null:n.toPlainString();}
  else if(base.equals("DATE")){LocalDate date=rows.getObject(i,LocalDate.class);value=date==null?null:date.toString();}
  else value=rows.getString(i);
  // csv-v2 reserves this literal even inside quotes. Reject instead of losing text.
  if(value!=null&&(value.equals("\\N")||value.indexOf(0)>=0||value.length()>65536))throw new Failure("UNREPRESENTABLE_VALUE");
  return value;
 }
 static Statement statement(Connection c,Limits limits)throws SQLException {var s=c.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);s.setQueryTimeout(limits.seconds());return s;}
 static void command(Connection c,Limits limits,String sql)throws SQLException {try(var s=statement(c,limits)){s.execute(sql);}}
 /** The factory must enforce connect/socket timeouts. Destination must not exist; same-filesystem atomic publication. */
 static Proof export(Connections factory,Table table,List<String> selection,Limits limits,Control control,Path destination)throws Failure {
  var columns=select(table,selection);Path scratch=null;boolean published=false;
  var timer=Executors.newSingleThreadScheduledExecutor(r->{var t=new Thread(r,"mysql-snapshot-deadline");t.setDaemon(true);return t;});
  var expiry=timer.schedule(control::expire,limits.seconds(),TimeUnit.SECONDS);
  try {
   if(Files.exists(destination,LinkOption.NOFOLLOW_LINKS))throw new Failure("DESTINATION_EXISTS");
   scratch=Files.createTempDirectory(destination.toAbsolutePath().getParent(),"mysql-pending-");Path csv=scratch.resolve("canonical.csv");
   long count=0;List<String> types=new ArrayList<>();
   try(Connection c=factory.open()) {
    control.attach(c);c.setReadOnly(true);c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);
    try {
     command(c,limits,"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
     // Acquire the table metadata lock before checking its engine; hold it to transaction end.
     try(var s=statement(c,limits);var r=s.executeQuery(sql(table,columns,0))){
      var meta=r.getMetaData();if(meta.getColumnCount()!=columns.size())throw new Failure("COLUMN_MISMATCH");
      for(int i=1;i<=columns.size();i++)types.add(type(meta,i));
     }
     try(var s=c.prepareStatement("SELECT ENGINE,TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?")) {
      s.setQueryTimeout(limits.seconds());s.setString(1,table.schema());s.setString(2,table.name());
      try(var r=s.executeQuery()){if(!r.next()||!"InnoDB".equals(r.getString(1))||!"BASE TABLE".equals(r.getString(2)))throw new Failure("UNSUPPORTED_TABLE");}
     }
     try(var s=statement(c,limits)) {
      s.setFetchSize(Integer.MIN_VALUE); // Connector/J streams rows; no whole-result buffering.
      try(var r=s.executeQuery(boundedSql(table,columns,types,limits.rows()+1));
          var out=new Bounded(Files.newOutputStream(csv,StandardOpenOption.CREATE_NEW),limits.bytes());
          var writer=new CSVPrinter(new OutputStreamWriter(out,StandardCharsets.UTF_8),CSVFormat.RFC4180.builder().setQuoteMode(QuoteMode.ALL).setNullString("\\N").get())) {
       writer.printRecord(columns.stream().map(Column::target).toList());
       while(r.next()) {
        control.check();if(++count>limits.rows())throw new Failure("ROW_LIMIT");
        var values=new ArrayList<String>();for(int i=1;i<=columns.size();i++)values.add(value(r,i,types.get(i-1)));
        writer.printRecord(values); // Duplicate rows are intentionally retained.
       }
      }
     }
     control.check();if(count==0)throw new Failure("EMPTY_TABLE");c.rollback();
    } catch(Exception e) {try{c.rollback();}catch(SQLException ignored){}throw e;}
   }
   control.check();var proof=new Proof(count,Files.size(csv),ExecutionInputs.sha(csv),columns.stream().map(Column::target).toList(),List.copyOf(types));
   var manifest=new LinkedHashMap<String,Object>();manifest.put("version",1);manifest.put("format","MYSQL_SNAPSHOT");manifest.put("importId",UUID.randomUUID().toString());
   manifest.put("schema",table.schema());manifest.put("table",table.name());manifest.put("mapping",columns);manifest.put("types",types);
   manifest.put("isolation","REPEATABLE READ; WITH CONSISTENT SNAPSHOT; READ ONLY");manifest.put("rows",proof.rows());manifest.put("bytes",proof.bytes());manifest.put("canonicalSha256",proof.sha256());
   new com.fasterxml.jackson.databind.ObjectMapper().writeValue(scratch.resolve("manifest.json").toFile(),manifest);
   if(!csv.toFile().setReadOnly()||!scratch.resolve("manifest.json").toFile().setReadOnly())throw new Failure("FREEZE_FAILED");
   // Cancellation and publication share the same monitor: cancellation cannot race a success.
   synchronized(control){control.check();Files.move(scratch,destination,StandardCopyOption.ATOMIC_MOVE);published=true;control.finish();}
   return proof;
  }catch(Failure e){throw e;}
   catch(Exception e){control.check();throw new Failure(e instanceof ByteLimit?"BYTE_LIMIT":"SNAPSHOT_FAILED");}
  finally {
   expiry.cancel(false);timer.shutdownNow();
   if(!published&&scratch!=null)try{Files.deleteIfExists(scratch.resolve("canonical.csv"));Files.deleteIfExists(scratch.resolve("manifest.json"));Files.deleteIfExists(scratch);}catch(IOException ignored){}
  }
 }
 static final class ByteLimit extends IOException {}
 static final class Bounded extends FilterOutputStream {
  final long limit;long count;
  Bounded(OutputStream out,long limit){super(out);this.limit=limit;}
  @Override public void write(int b)throws IOException {if(++count>limit)throw new ByteLimit();out.write(b);}
  @Override public void write(byte[] b,int off,int len)throws IOException {if((count+=len)>limit)throw new ByteLimit();out.write(b,off,len);}
 }
}
