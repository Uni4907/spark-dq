package cn.sparkdq.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class PreparedInputTest {
 @TempDir Path root;
 Path manifest;Map<String,Object> value;Path data;
 @BeforeEach void fixture()throws Exception{
  Files.createDirectory(root.resolve("data"));data=root.resolve("data/part-000.csv");Files.writeString(data,"id\n1\n");
  value=new LinkedHashMap<>(Map.of("version",1,"format","CSV","semanticsVersion","csv-v2","sourceSha256","a".repeat(64),
   "columns",List.of("id"),"rowCount",1,"files",List.of(Map.of("path","data/part-000.csv","size",Files.size(data),"sha256",PreparedInput.sha(data)))));
  manifest=root.resolve("manifest.json");save();
 }
 void save()throws Exception{PreparedInput.JSON.writeValue(manifest.toFile(),value);}
 PreparedInput.Manifest load()throws Exception{return PreparedInput.load(manifest,PreparedInput.sha(manifest));}
 @Test void validExplicitManifest()throws Exception{assertEquals(1,load().rows());assertEquals(data.toString(),load().files()[0]);}
 @Test void checksumPinAndDataMutation()throws Exception{
  assertThrows(IllegalArgumentException.class,()->PreparedInput.load(manifest,"b".repeat(64)));
  Files.writeString(data,"id\n2\n");assertThrows(IllegalArgumentException.class,this::load);
 }
 @Test void rejectsTraversalAndDuplicateFiles()throws Exception{
  value.put("files",List.of(Map.of("path","../part-000.csv")));save();assertThrows(IllegalArgumentException.class,this::load);
  fixtureResetFiles();var files=(List<?>)value.get("files");value.put("files",List.of(files.get(0),files.get(0)));save();
  assertThrows(IllegalArgumentException.class,this::load);
 }
 void fixtureResetFiles()throws Exception{value.put("files",List.of(Map.of("path","data/part-000.csv","size",Files.size(data),"sha256",PreparedInput.sha(data))));}
 @Test void rejectsUnknownVersionFormatAndNumericRows()throws Exception{
  for(var pair:List.of(Map.entry("version",2),Map.entry("format","ORC"),Map.entry("rowCount",1.5),Map.entry("semanticsVersion","csv-v1"))){
   Object old=value.put(pair.getKey(),pair.getValue());save();assertThrows(IllegalArgumentException.class,this::load);value.put(pair.getKey(),old);
  }
 }
 @Test void rejectsAmbiguousOrUnsafeColumns()throws Exception{
  for(var columns:List.of(List.of("id","id"),List.of("a.b"),List.of("__dq_x"),List.of())){
   value.put("columns",columns);save();assertThrows(IllegalArgumentException.class,this::load);
  }
 }
 @Test void rejectsNonStringAndReorderedSchemas(){
  var string=org.apache.spark.sql.types.DataTypes.StringType;
  var integer=org.apache.spark.sql.types.DataTypes.IntegerType;
  var schema=org.apache.spark.sql.types.DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
   org.apache.spark.sql.types.DataTypes.createStructField("id",integer,true)});
  assertThrows(IllegalArgumentException.class,()->PreparedInput.validateSchema(schema,new String[]{"id"}));
  var other=org.apache.spark.sql.types.DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
   org.apache.spark.sql.types.DataTypes.createStructField("other",string,true)});
  assertThrows(IllegalArgumentException.class,()->PreparedInput.validateSchema(other,new String[]{"id"}));
 }
 @Test void rejectsSymlink()throws Exception{
  Path outside=root.resolve("original.csv");Files.move(data,outside);Files.createSymbolicLink(data,outside);
  assertThrows(IllegalArgumentException.class,this::load);
 }
 @Test void canonicalHeaderAcceptsCompletePlainOrQuotedRecords()throws Exception{
  for(String header:List.of("id,name","\"id\",\"name\""))for(String suffix:List.of("","\n1,A\n","\r\n1,A\r\n"))
   assertTrue(header(header+suffix,"id","name"),header+suffix);
 }
 @Test void noncanonicalHeadersUseSparkFallback()throws Exception{
  for(String text:List.of("\ufeffid,name\n", "id,\"name\"\n", " id,name\n", "id,name\r1,A\r"))
   assertFalse(header(text,"id","name"),text);
 }
 @Test void mismatchesAndPrefixesNeverBypassParser()throws Exception{
  for(String text:List.of("name,id\n", "ID,name\n", "id,nameExtra\n", "id,name,extra\n", "\"id\",\"name", "\"id\",\"name\nother\"\n", "id,name"+"x".repeat(10000)))
   assertFalse(header(text,"id","name"),text.substring(0,Math.min(40,text.length())));
 }
 @Test void headerRecognitionReadsBoundedBytes()throws Exception{
  var bytes=new java.io.ByteArrayInputStream(("id"+"x".repeat(100000)).getBytes(java.nio.charset.StandardCharsets.UTF_8));
  int before=bytes.available();assertFalse(PreparedInput.canonicalHeader(bytes,new String[]{"id"}));
  assertEquals(6,before-bytes.available());
 }
 @Test void largestValidCanonicalHeader()throws Exception{
  String[] names=java.util.stream.IntStream.range(0,100).mapToObj(i->"c"+i+"x".repeat(60)).toArray(String[]::new);
  assertTrue(header("\""+String.join("\",\"",names)+"\"\r\n",names));
 }
 boolean header(String text,String... names)throws Exception{
  return PreparedInput.canonicalHeader(new java.io.ByteArrayInputStream(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)),names);
 }
}
