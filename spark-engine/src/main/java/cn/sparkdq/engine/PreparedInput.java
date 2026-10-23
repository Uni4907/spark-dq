package cn.sparkdq.engine;

import com.fasterxml.jackson.databind.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.io.*;
import java.security.MessageDigest;
import java.util.*;
import static org.apache.spark.sql.functions.*;

/** Offline prepared input contract. HTTP tasks continue using their immutable CSV snapshot. */
public final class PreparedInput {
 static final ObjectMapper JSON=new ObjectMapper();
 record Manifest(Path root,String format,String[] columns,long rows,String[] files){}
 static String sha(Path path)throws Exception{
  var digest=MessageDigest.getInstance("SHA-256");
  try(var stream=Files.newInputStream(path)){byte[] bytes=new byte[65536];int n;while((n=stream.read(bytes))!=-1)digest.update(bytes,0,n);}
  return HexFormat.of().formatHex(digest.digest());
 }
 static void require(boolean valid,String message){if(!valid)throw new IllegalArgumentException(message);}
 static String[] columns(JsonNode node){
  require(node.isArray()&&node.size()>0&&node.size()<=100,"Invalid prepared columns");
  var names=new ArrayList<String>();var seen=new HashSet<String>();
  for(var value:node){
   require(value.isTextual()&&value.asText().matches("[A-Za-z][A-Za-z0-9_]{0,63}")&&seen.add(value.asText()),"Invalid prepared column");
   names.add(value.asText());
  }
  return names.toArray(String[]::new);
 }
 static Manifest load(Path path,String expectedSha)throws Exception{
  require(expectedSha.matches("[0-9a-f]{64}"),"A pinned manifest SHA-256 is required");
  path=path.toAbsolutePath().normalize();
  require(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)&&Files.size(path)<=1048576,"Invalid prepared manifest");
  require(path.toRealPath().equals(path),"Manifest symlinks are not supported");
  require(sha(path).equals(expectedSha),"Prepared manifest checksum mismatch");
  var node=JSON.readTree(path.toFile());
  require(node.path("version").isIntegralNumber()&&node.path("version").asInt()==1,"Unsupported prepared manifest version");
  String format=node.path("format").asText();
  require(Set.of("CSV","PARQUET").contains(format),"Unsupported prepared format");
  require("csv-v2".equals(node.path("semanticsVersion").asText()),"Prepared input requires csv-v2 semantics");
  require(node.path("sourceSha256").asText().matches("[0-9a-f]{64}"),"Invalid source fingerprint");
  String[] names=columns(node.path("columns"));
  require(node.path("rowCount").canConvertToLong()&&node.path("rowCount").isIntegralNumber()&&node.path("rowCount").asLong()>=0,"Invalid row count");
  var entries=node.path("files");require(entries.isArray()&&entries.size()>0&&entries.size()<=256,"Invalid prepared file list");
  Path root=path.getParent();var files=new ArrayList<String>();var seen=new HashSet<String>();long bytes=0;
  for(var entry:entries){
   String name=entry.path("path").asText(),suffix=format.equals("CSV")?".csv":".parquet";
   require(name.matches("data/part-[A-Za-z0-9_.-]+")&&name.endsWith(suffix)&&seen.add(name),"Invalid prepared file path");
   Path file=root.resolve(name);
   require(Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)&&file.toRealPath().equals(file),"Missing file or symlink in prepared input");
   require(entry.path("size").isIntegralNumber()&&entry.path("size").canConvertToLong()&&entry.path("size").asLong()==Files.size(file),"Prepared file size mismatch");
   bytes=Math.addExact(bytes,Files.size(file));require(bytes<=256L*1024*1024,"Prepared input exceeds 256 MiB");
   require(entry.path("sha256").asText().matches("[0-9a-f]{64}")&&sha(file).equals(entry.path("sha256").asText()),"Prepared file checksum mismatch");
   files.add(file.toString());
  }
  return new Manifest(root,format,names,node.path("rowCount").asLong(),files.toArray(String[]::new));
 }
 static Dataset<Row> read(SparkSession spark,Manifest manifest){
  Dataset<Row> df;
  if(manifest.format().equals("CSV")){
   // A merged multi-path scan may only inspect one header even with enforceSchema=false.
   // Canonical ASCII headers can be checked without launching one inference job per part.
   // Other valid CSV spellings retain Spark's existing parser and compatibility behavior.
   for(String file:manifest.files()){
    boolean canonical;
    try(var stream=Files.newInputStream(Path.of(file))){canonical=canonicalHeader(stream,manifest.columns());}
    catch(IOException error){throw new UncheckedIOException("Cannot read prepared CSV header",error);}
    if(!canonical)require(Arrays.equals(Engine.readCsv(spark,file,true).columns(),manifest.columns()),"Prepared CSV header mismatch");
   }
   var fields=Arrays.stream(manifest.columns()).map(name->DataTypes.createStructField(name,DataTypes.StringType,true)).toArray(StructField[]::new);
   df=spark.read().schema(DataTypes.createStructType(fields)).option("header","true").option("enforceSchema","false")
    .option("multiLine","true").option("mode","FAILFAST").option("encoding","UTF-8")
    .option("lineSep","\n")
    .option("nullValue","\\N").option("emptyValue","").option("escape","\"")
    .option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace","false").csv(manifest.files());
  }else{
   // Check every footer: mergeSchema=false alone can conceal a missing column in one part.
   for(String file:manifest.files())validateSchema(spark.read().parquet(file).schema(),manifest.columns());
   df=spark.read().option("mergeSchema","false").parquet(manifest.files());
  }
  validateSchema(df.schema(),manifest.columns());
  return df;
 }
 /** Bounded recognition, not a general CSV parser. Unrecognized spellings fall back to Spark. */
 static boolean canonicalHeader(InputStream stream,String[] names)throws IOException{
  String plain=String.join(",",names);
  String quoted="\""+String.join("\",\"",names)+"\"";
  // Manifest column names are validated ASCII identifiers, at most 100 x 64 characters.
  byte[] prefix=stream.readNBytes(quoted.length()+2);
  int end=0;while(end<prefix.length&&prefix[end]!='\n')end++;
  if(end==prefix.length&&prefix.length==quoted.length()+2)return false;
  if(end>0&&prefix[end-1]=='\r')end--;
  String header=new String(prefix,0,end,StandardCharsets.US_ASCII);
  return header.equals(plain)||header.equals(quoted);
 }
 static void validateSchema(StructType schema,String[] names){
  require(Arrays.equals(schema.fieldNames(),names),"Prepared column order/schema mismatch");
  for(var field:schema.fields())require(field.dataType().equals(DataTypes.StringType),"Prepared fields must be strings");
 }
 /** CSV -> new immutable multi-file CSV or string-only Parquet, with exact multiset readback. */
 public static void main(String[] args)throws Exception{
  long started=System.nanoTime();var request=JSON.readTree(Path.of(args[0]).toFile());
  Path input=Path.of(request.path("input").asText()).toAbsolutePath().normalize();
  Path output=Path.of(request.path("output").asText()).toAbsolutePath().normalize();
  String format=request.path("format").asText();int parts=request.path("partitions").asInt(0);
  require(Set.of("CSV","PARQUET").contains(format)&&parts>=1&&parts<=16,"Invalid preparation format/partitions");
  require(Files.isRegularFile(input,LinkOption.NOFOLLOW_LINKS)&&input.toRealPath().equals(input)&&Files.size(input)<=20L*1024*1024,"Preparation requires a local CSV up to 20 MiB");
  require(!Files.exists(output,LinkOption.NOFOLLOW_LINKS)&&Files.isDirectory(output.getParent()),"Preparation output must be new");
  String sourceSha=sha(input);Files.createDirectory(output);
  SparkSession spark=SparkSession.builder().appName("SparkDQ-prepare").master(System.getenv().getOrDefault("SPARK_MASTER","local[2]"))
   .config("spark.sql.caseSensitive","true").config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","2")
   .config("spark.driver.bindAddress","127.0.0.1").config("spark.driver.host","127.0.0.1").getOrCreate();
  spark.sparkContext().setLogLevel("WARN");
  try{
   var df=Engine.readCsv(spark,input.toString(),true).cache();
   String[] names=columns(JSON.valueToTree(df.columns()));long count=df.count();
   var writer=df.repartition(parts).write().mode("errorifexists");
   if(format.equals("CSV"))writer.option("header","true").option("quoteAll","true").option("escape","\"")
    .option("nullValue","\\N").option("emptyValue","\"\"").option("ignoreLeadingWhiteSpace","false")
    .option("ignoreTrailingWhiteSpace","false").csv(output.resolve("data").toString());
   else writer.option("compression","snappy").parquet(output.resolve("data").toString());
   var files=new ArrayList<Map<String,Object>>();
   try(var paths=Files.list(output.resolve("data"))){
    for(Path file:paths.filter(p->p.getFileName().toString().startsWith("part-")).sorted().toList()){
     files.add(Map.of("path","data/"+file.getFileName(),"size",Files.size(file),"sha256",sha(file)));
    }
   }
   var manifest=new LinkedHashMap<String,Object>();
   manifest.put("version",1);manifest.put("format",format);manifest.put("semanticsVersion","csv-v2");
   manifest.put("sourceSha256",sourceSha);manifest.put("columns",names);manifest.put("rowCount",count);manifest.put("files",files);
   Path temporary=output.resolve("manifest.pending.json");JSON.writeValue(temporary.toFile(),manifest);
   var prepared=load(temporary,sha(temporary));var back=read(spark,prepared).cache();
   require(back.count()==count&&df.exceptAll(back).limit(1).count()==0&&back.exceptAll(df).limit(1).count()==0,"Preparation readback values differ");
   require(sourceSha.equals(sha(input)),"Source changed during preparation");
   Files.move(temporary,output.resolve("manifest.json"),StandardCopyOption.ATOMIC_MOVE);
   var result=Map.of("verified",true,"format",format,"rows",count,"files",files.size(),"manifestSha256",sha(output.resolve("manifest.json")),"preparationMs",WorkerMetrics.millisSince(started));
   JSON.writeValue(output.resolve("preparation.json").toFile(),result);
   System.out.println(JSON.writeValueAsString(result));
   back.unpersist();df.unpersist();
  }finally{spark.stop();}
 }
}
