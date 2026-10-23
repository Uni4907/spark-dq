package cn.sparkdq.engine;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.nio.file.*;
import java.util.*;
import java.math.BigDecimal;
import static org.apache.spark.sql.functions.*;

/** Acceptance fixture producer only; excluded from the application JAR. */
public class NativeParquetFixtures {
 static void write(SparkSession spark,Path root,String name,StructType schema,List<Row> rows)throws Exception{
  Path dir=root.resolve(name+"-parts");spark.createDataFrame(rows,schema).coalesce(1).write().mode("errorifexists").parquet(dir.toString());
  try(var files=Files.list(dir)){Path part=files.filter(p->p.getFileName().toString().endsWith(".parquet")).findFirst().orElseThrow();Files.copy(part,root.resolve(name+".parquet"));}
 }
 static StructField field(String name,DataType type){return DataTypes.createStructField(name,type,true);}
 public static void main(String[] args)throws Exception{
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"));Path root=Path.of(args[0]);Files.createDirectories(root);
  var spark=SparkSession.builder().appName("SparkDQ-native-fixtures").master("local[2]").config("spark.ui.enabled","false").config("spark.sql.session.timeZone","UTC").config("spark.sql.caseSensitive","true").config("spark.sql.parquet.datetimeRebaseModeInWrite","CORRECTED").config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1").getOrCreate();spark.sparkContext().setLogLevel("WARN");
  try{
   var schema=DataTypes.createStructType(new StructField[]{field("id",DataTypes.LongType),field("name",DataTypes.StringType),field("amount",DataTypes.createDecimalType(38,10)),field("day",DataTypes.DateType),field("active",DataTypes.BooleanType),field("small",DataTypes.ShortType),field("tiny",DataTypes.ByteType),field("number",DataTypes.IntegerType),field("note",DataTypes.StringType)});
   var rows=List.of(RowFactory.create(1L,null,new BigDecimal("9.0000000000"),java.sql.Date.valueOf("2024-02-29"),true,(short)2,(byte)1,3,"中文🙂,\n\"quoted\""),
    RowFactory.create(1L,"Alice",new BigDecimal("10.0000000000"),java.sql.Date.valueOf("2024-03-01"),false,(short)-2,(byte)-1,-3,"winner"),
    RowFactory.create(Long.MIN_VALUE,"",new BigDecimal("1234567890123456789012345678.1234567890"),java.sql.Date.valueOf("1582-10-15"),null,null,null,null,""),
    RowFactory.create(Long.MAX_VALUE,"   ",new BigDecimal("-1.2300000000"),java.sql.Date.valueOf("9999-12-31"),true,(short)32767,(byte)127,Integer.MAX_VALUE," \n "),
    RowFactory.create(null,"N",null,null,false,null,null,null,"literal \\N is fine"),
    RowFactory.create(null,"N2",new BigDecimal("2.0000000000"),java.sql.Date.valueOf("2026-01-02"),true,(short)0,(byte)0,0,"high"));
   write(spark,root,"typed",schema,rows);
   write(spark,root,"empty",schema,List.of());
   for(var entry:List.of(Map.entry("sentinel","\\N"),Map.entry("nul","bad\u0000value"),Map.entry("longcell","x".repeat(65537)),Map.entry("crlf","CR\r\nLF"),Map.entry("carriage","CR\r")))
    write(spark,root,entry.getKey(),DataTypes.createStructType(new StructField[]{field("id",DataTypes.StringType)}),List.of(RowFactory.create(entry.getValue())));
   write(spark,root,"float",DataTypes.createStructType(new StructField[]{field("id",DataTypes.DoubleType)}),List.of(RowFactory.create(Double.NaN),RowFactory.create(Double.POSITIVE_INFINITY)));
   write(spark,root,"timestamp",DataTypes.createStructType(new StructField[]{field("id",DataTypes.TimestampType)}),List.of(RowFactory.create(java.sql.Timestamp.valueOf("2026-10-19 01:02:03.123456"))));
   write(spark,root,"nested",DataTypes.createStructType(new StructField[]{field("id",DataTypes.createArrayType(DataTypes.StringType))}),List.of(RowFactory.create(List.of("x","y"))));
   write(spark,root,"badname",DataTypes.createStructType(new StructField[]{field("bad.name",DataTypes.StringType)}),List.of(RowFactory.create("x")));
   write(spark,root,"expanded",DataTypes.createStructType(new StructField[]{field("id",DataTypes.StringType)}),java.util.Collections.nCopies(400,RowFactory.create("x".repeat(60000))));
   // This compressed input is tiny; canonical expansion must fail before publication.
   spark.range(1).select(expr("CAST(unhex('c328') AS STRING)").alias("id")).coalesce(1).write().mode("errorifexists").parquet(root.resolve("invalidutf8-parts").toString());
   try(var files=Files.list(root.resolve("invalidutf8-parts"))){Files.copy(files.filter(p->p.getFileName().toString().endsWith(".parquet")).findFirst().orElseThrow(),root.resolve("invalidutf8.parquet"));}
   spark.range(1).select(to_date(lit("1582-10-14")).alias("id")).coalesce(1).write().mode("errorifexists").parquet(root.resolve("old-date-parts").toString());
   try(var files=Files.list(root.resolve("old-date-parts"))){Files.copy(files.filter(p->p.getFileName().toString().endsWith(".parquet")).findFirst().orElseThrow(),root.resolve("old-date.parquet"));}
   System.out.println("Created 14 native Parquet fixtures");
  }finally{spark.stop();}
 }
}
