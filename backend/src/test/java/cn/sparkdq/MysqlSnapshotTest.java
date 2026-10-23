package cn.sparkdq;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MysqlSnapshotTest {
 @TempDir Path root;
 final MysqlSnapshot.Table table=new MysqlSnapshot.Table("business","fixture",List.of(new MysqlSnapshot.Column("金额","amount"),new MysqlSnapshot.Column("text","text")));
 final MysqlSnapshot.Limits limits=new MysqlSnapshot.Limits(10,1024,5);
 @Test void rejectsIdentifiersAndDuplicateMappings(){
  assertThrows(IllegalArgumentException.class,()->new MysqlSnapshot.Column("x` FROM users--","x"));
  assertThrows(IllegalArgumentException.class,()->new MysqlSnapshot.Column("金额","金额"));
  assertThrows(IllegalArgumentException.class,()->new MysqlSnapshot.Table("a","b",List.of(new MysqlSnapshot.Column("x","x"),new MysqlSnapshot.Column("y","x"))));
 }
 @Test void selectionMustBelongToAllowlist(){assertThrows(IllegalArgumentException.class,()->MysqlSnapshot.select(table,List.of("secret")));assertThrows(IllegalArgumentException.class,()->MysqlSnapshot.select(table,List.of("text","text")));}
 @Test void quotesSourceAndMapsColumns(){assertEquals("SELECT `金额`,`text` FROM `business`.`fixture` LIMIT 11",MysqlSnapshot.sql(table,MysqlSnapshot.select(table,List.of("金额","text")),11));}
 @Test void validatesResourceCeilings(){assertThrows(IllegalArgumentException.class,()->new MysqlSnapshot.Limits(0,1024,5));assertThrows(IllegalArgumentException.class,()->new MysqlSnapshot.Limits(10,MysqlSnapshot.MAX_BYTES+1,5));assertThrows(IllegalArgumentException.class,()->new MysqlSnapshot.Limits(10,1024,301));}
 @Test void rejectsLossyTypes()throws Exception {var meta=mock(ResultSetMetaData.class);for(String type:List.of("DOUBLE","FLOAT","TIMESTAMP","JSON","BIT","CHAR","BLOB","ENUM")){when(meta.getColumnTypeName(1)).thenReturn(type);assertThrows(MysqlSnapshot.Failure.class,()->MysqlSnapshot.type(meta,1));}}
 @Test void keepsDecimalAndNullExact()throws Exception {var rows=mock(ResultSet.class);when(rows.getBigDecimal(1)).thenReturn(new java.math.BigDecimal("9007199254740993.1234567890"));assertEquals("9007199254740993.1234567890",MysqlSnapshot.value(rows,1,"DECIMAL"));assertNull(MysqlSnapshot.value(rows,2,"DECIMAL"));}
 @Test void rejectsReservedNullMarkerAndNul()throws Exception {var rows=mock(ResultSet.class);for(String v:List.of("\\N","a\0b","x".repeat(65537))){when(rows.getString(1)).thenReturn(v);assertThrows(MysqlSnapshot.Failure.class,()->MysqlSnapshot.value(rows,1,"TEXT"));}}
 @Test void keepsWhitespaceAndNewlines()throws Exception {var rows=mock(ResultSet.class);for(String v:List.of(""," ","中文\n换行\r\n\t","9007199254740993")){when(rows.getString(1)).thenReturn(v);assertEquals(v,MysqlSnapshot.value(rows,1,"VARCHAR"));}}
 @Test void byteBoundDoesNotSilentlyTruncate()throws Exception {var out=new java.io.ByteArrayOutputStream();var bounded=new MysqlSnapshot.Bounded(out,3);bounded.write(new byte[]{1,2,3});assertThrows(MysqlSnapshot.ByteLimit.class,()->bounded.write(4));assertEquals(3,out.size());}
 @Test void sanitizesConnectionErrorsAndRemovesScratch()throws Exception {
  var failure=assertThrows(MysqlSnapshot.Failure.class,()->MysqlSnapshot.export(()->{throw new SQLException("secret-password@host");},table,List.of("text"),limits,new MysqlSnapshot.Control(),root.resolve("result")));
  assertEquals("SNAPSHOT_FAILED",failure.getMessage());assertNull(failure.getCause());try(var files=Files.list(root)){assertEquals(0,files.count());}
 }
 @Test void neverOverwritesDestination()throws Exception {var target=Files.createDirectory(root.resolve("result"));Files.writeString(target.resolve("keep"),"original");var f=assertThrows(MysqlSnapshot.Failure.class,()->MysqlSnapshot.export(()->{throw new AssertionError();},table,List.of("text"),limits,new MysqlSnapshot.Control(),target));assertEquals("DESTINATION_EXISTS",f.code);assertEquals("original",Files.readString(target.resolve("keep")));}
 @Test void preCancelledControlPublishesNothing()throws Exception {var control=new MysqlSnapshot.Control();control.cancel();var connection=mock(Connection.class);var f=assertThrows(MysqlSnapshot.Failure.class,()->MysqlSnapshot.export(()->connection,table,List.of("text"),limits,control,root.resolve("result")));assertEquals("CANCELLED",f.code);assertFalse(Files.exists(root.resolve("result")));}
}
