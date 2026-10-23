package cn.sparkdq.engine;

import com.fasterxml.jackson.databind.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import static org.apache.spark.sql.functions.col;

/** Native typed Parquet -> bounded csv-v2, with explicit lossless scalar normalization. */
public final class ParquetImport {
 static final ObjectMapper JSON=new ObjectMapper();
 static final long MAX_BYTES=20L*1024*1024,MAX_ROWS=1_000_000;
 record Request(Path input,Path output,String sha){}
 static void require(boolean yes,String message){if(!yes)throw new IllegalArgumentException(message);}
 static Request request(JsonNode node)throws Exception{
  require(node.isObject()&&node.size()==3&&node.path("input").isTextual()&&node.path("output").isTextual()&&node.path("sourceSha256").isTextual(),"Import request requires exactly input, output, sourceSha256");
  Path input=Path.of(node.path("input").asText()),output=Path.of(node.path("output").asText());String pin=node.path("sourceSha256").asText();
  require(input.isAbsolute()&&output.isAbsolute()&&input.equals(input.normalize())&&output.equals(output.normalize()),"Import paths must be absolute and normalized");
  require(input.toString().matches("[/A-Za-z0-9_.-]+")&&input.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".parquet"),"Worker requires a controlled non-glob Parquet path");
  require(Files.isRegularFile(input,LinkOption.NOFOLLOW_LINKS)&&input.toRealPath().equals(input)&&Files.size(input)>=12&&Files.size(input)<=MAX_BYTES,"Parquet source must be a regular non-link file of 12 bytes..20MiB");
  require(!Files.exists(output,LinkOption.NOFOLLOW_LINKS)&&Files.isDirectory(output.getParent())&&output.getParent().toRealPath().equals(output.getParent()),"Import output must be new, with a non-link parent");
  require(pin.matches("[0-9a-f]{64}")&&pin.equals(PreparedInput.sha(input)),"Pinned source SHA-256 mismatch");
  try(var stream=Files.newByteChannel(input)){
   var start=ByteBuffer.allocate(4);var end=ByteBuffer.allocate(4);stream.read(start);stream.position(Files.size(input)-4);stream.read(end);
   require(Arrays.equals(start.array(),new byte[]{'P','A','R','1'})&&Arrays.equals(end.array(),start.array()),"Only unencrypted PAR1 Parquet is supported");
  }
  return new Request(input,output,pin);
 }
 static List<Map<String,Object>> schema(StructType schema){
  require(schema.fields().length>=1&&schema.fields().length<=100,"Parquet requires 1..100 columns");
  var names=new HashSet<String>();var result=new ArrayList<Map<String,Object>>();
  for(var field:schema.fields()){
   require(field.name().matches("[A-Za-z][A-Za-z0-9_]{0,63}")&&names.add(field.name()),"Parquet fields must have unique CSV-compatible names");
   DataType type=field.dataType();
   require(type.equals(DataTypes.StringType)||type.equals(DataTypes.BooleanType)||type.equals(DataTypes.ByteType)||type.equals(DataTypes.ShortType)||type.equals(DataTypes.IntegerType)||type.equals(DataTypes.LongType)||type.equals(DataTypes.DateType)||type instanceof DecimalType,
    "Unsupported Parquet type for "+field.name()+": "+type.catalogString()+" (no implicit float/timestamp/complex conversion)");
   result.add(Map.of("name",field.name(),"type",type.catalogString(),"nullable",field.nullable()));
  }
  return List.copyOf(result);
 }
 static String text(Object value,DataType type)throws Exception{
  if(value==null)return null;
  String result;
  if(type.equals(DataTypes.StringType)){
   // Spark exposes the original UTF8String bytes through a binary projection; do not replace malformed bytes.
   require(value instanceof byte[],"String normalization requires original UTF-8 bytes");
   result=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap((byte[])value)).toString();
  }else if(type instanceof DecimalType)result=((BigDecimal)value).toPlainString();
  else if(type.equals(DataTypes.DateType)){
   LocalDate date=LocalDate.parse((String)value);
   require(!date.isBefore(LocalDate.of(1582,10,15))&&date.getYear()<=9999,"Date outside supported Gregorian range 1582-10-15..9999-12-31");result=date.toString();
  }else result=value.toString();
  require(result.length()<=65536&&result.indexOf(0)<0,"Parquet cell contains NUL or exceeds 65536 characters");
  require(result.indexOf('\r')<0,"Carriage return cannot round-trip through csv-v2; normalize explicitly before importing");
  require(!result.equals("\\N"),"Non-null Parquet value collides with reserved CSV null marker \\N");
  return result;
 }
 static final class Bounded extends FilterOutputStream {
  long bytes;
  Bounded(OutputStream out){super(out);}
  public void write(int b)throws IOException{require(bytes+1<=MAX_BYTES,"Canonical CSV exceeds20MiB");out.write(b);bytes++;}
  public void write(byte[] b,int offset,int length)throws IOException{require(bytes+length<=MAX_BYTES,"Canonical CSV exceeds20MiB");out.write(b,offset,length);bytes+=length;}
 }
 static void record(Writer out,List<String> values)throws IOException{
  for(int i=0;i<values.size();i++){
   if(i>0)out.write(',');out.write('"');String value=values.get(i)==null?"\\N":values.get(i);
   out.write(value.replace("\"","\"\""));out.write('"');
  }
  out.write("\r\n");
 }
 public static void main(String[] args)throws Exception{
  long started=System.nanoTime();require(args.length==1,"One request JSON path is required");Path settings=Path.of(args[0]);
  require(Files.isRegularFile(settings,LinkOption.NOFOLLOW_LINKS)&&Files.size(settings)<=16384,"Invalid import request file");
  Request request=request(JSON.readTree(settings.toFile()));Files.createDirectory(request.output());
  // This owns a standalone JVM; lock both JDBC Row date conversion and Spark formatting to UTC.
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  SparkSession spark=SparkSession.builder().appName("SparkDQ-native-parquet-import").master(System.getenv().getOrDefault("SPARK_MASTER","local[2]"))
   .config("spark.sql.caseSensitive","true").config("spark.sql.session.timeZone","UTC").config("spark.sql.parquet.datetimeRebaseModeInRead","EXCEPTION")
   .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","2")
   .config("spark.driver.bindAddress","127.0.0.1").config("spark.driver.host","127.0.0.1").getOrCreate();
  spark.sparkContext().setLogLevel("WARN");
  try{
   var nativeData=spark.read().parquet(request.input().toString());var types=nativeData.schema().fields();var descriptors=schema(nativeData.schema());String[] names=nativeData.columns();
   String[] files=nativeData.inputFiles();require(files.length==1&&Path.of(java.net.URI.create(files[0])).equals(request.input()),"Parquet scan must contain exactly the pinned file");
   var projected=nativeData.select(Arrays.stream(types).map(f->f.dataType().equals(DataTypes.StringType)?col(f.name()).cast(DataTypes.BinaryType).alias(f.name()):f.dataType().equals(DataTypes.DateType)?col(f.name()).cast(DataTypes.StringType).alias(f.name()):col(f.name())).toArray(Column[]::new));
   Path pending=request.output().resolve("canonical.pending.csv");long rows=0;
   try(var out=new BufferedWriter(new OutputStreamWriter(new Bounded(Files.newOutputStream(pending,StandardOpenOption.CREATE_NEW)),StandardCharsets.UTF_8))){
    record(out,Arrays.asList(names));var iterator=projected.toLocalIterator();
    while(iterator.hasNext()){
     require(rows<MAX_ROWS,"Parquet exceeds one million records");Row row=iterator.next();var values=new ArrayList<String>();
     for(int i=0;i<types.length;i++)values.add(text(row.isNullAt(i)?null:row.get(i),types[i].dataType()));
     record(out,values);rows++;
    }
   }
   require(rows>0,"Native registration requires at least one record");
   var back=Engine.readCsv(spark,pending.toString(),true).cache();
   var normalized=nativeData.select(Arrays.stream(names).map(n->col(n).cast(DataTypes.StringType).alias(n)).toArray(Column[]::new));
   require(Arrays.equals(names,back.columns())&&back.count()==rows&&normalized.exceptAll(back).limit(1).count()==0&&back.exceptAll(normalized).limit(1).count()==0,"Native Parquet canonical string multiset readback mismatch");
   require(request.sha().equals(PreparedInput.sha(request.input())),"Native source changed during import");
   var manifest=new LinkedHashMap<String,Object>();
   manifest.put("version",1);manifest.put("format","PARQUET_NATIVE");manifest.put("semanticsVersion","csv-v2");manifest.put("verified",true);
   manifest.put("sourceSha256",request.sha());manifest.put("sourceBytes",Files.size(request.input()));manifest.put("rowCount",rows);manifest.put("columns",descriptors);
   manifest.put("canonical",Map.of("path","canonical.csv","sha256",PreparedInput.sha(pending),"size",Files.size(pending)));
   manifest.put("conversion",Map.of("version",1,"timezone","UTC","decimal","PLAIN_WITH_SCALE","integer","BASE10","boolean","LOWERCASE","date","GREGORIAN_1582_10_15_TO_9999","string","STRICT_UTF8","order","UNSPECIFIED"));
   Path manifestPending=request.output().resolve("manifest.pending.json");JSON.writeValue(manifestPending.toFile(),manifest);
   Files.move(pending,request.output().resolve("canonical.csv"),StandardCopyOption.ATOMIC_MOVE);request.output().resolve("canonical.csv").toFile().setReadOnly();
   Files.move(manifestPending,request.output().resolve("manifest.json"),StandardCopyOption.ATOMIC_MOVE);request.output().resolve("manifest.json").toFile().setReadOnly();
   System.out.println(JSON.writeValueAsString(Map.of("verified",true,"rows",rows,"manifestSha256",PreparedInput.sha(request.output().resolve("manifest.json")),"elapsedMs",WorkerMetrics.millisSince(started))));back.unpersist();
  }finally{spark.stop();}
 }
}
