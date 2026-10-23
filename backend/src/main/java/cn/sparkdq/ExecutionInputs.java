package cn.sparkdq;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

/** Execution copies never replace the registered canonical CSV. */
public final class ExecutionInputs {
 @JsonDeserialize(using=Reader.class)
 public record Plan(String format,int partitions){}
 public record Frozen(int version,String format,int partitions,long rowCount,List<String> columns){}
 public static class Reader extends JsonDeserializer<Plan>{
  public Plan deserialize(com.fasterxml.jackson.core.JsonParser parser,DeserializationContext context)throws java.io.IOException{
   JsonNode node=parser.readValueAsTree();
   if(!node.isObject()||node.size()!=2||!node.path("format").isTextual()||!node.path("partitions").isIntegralNumber()||!node.path("partitions").canConvertToInt())
    return context.reportInputMismatch(Plan.class,"执行副本只接受 format 和整数 partitions");
   return new Plan(node.path("format").asText(),node.path("partitions").asInt());
  }
 }
 static Plan validate(Plan plan){
  if(plan==null)return null;
  if(plan.format()==null||!Set.of("CSV_SINGLE","CSV","PARQUET").contains(plan.format())||plan.partitions()<1||plan.partitions()>16||
    (plan.format().equals("CSV_SINGLE")&&plan.partitions()!=1))throw Datasets.bad("执行格式须为 CSV_SINGLE/CSV/PARQUET，分片数 1..16；原始 CSV 固定为 1");
  return plan;
 }
 static Frozen freeze(Plan plan,Map<String,Object> data,String semantics){
  validate(plan);if(plan==null||plan.format().equals("CSV_SINGLE"))return null;
  if(!"csv-v2".equals(semantics))throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.CONFLICT,"历史统计口径仅支持原始 CSV；请先新建检测");
  @SuppressWarnings("unchecked") var columns=(List<String>)data.get("columns");
  return new Frozen(1,plan.format(),plan.partitions(),((Number)data.get("row_count")).longValue(),List.copyOf(columns));
 }
 static Plan inherited(Map<String,Object> parent){
  if(parent.get("snapshot") instanceof Map<?,?> snapshot&&snapshot.get("executionInput") instanceof Map<?,?> plan)
   return validate(new Plan(plan.get("format").toString(),((Number)plan.get("partitions")).intValue()));
  return null;
 }
 static String sha(Path path)throws Exception{
  var digest=MessageDigest.getInstance("SHA-256");
  try(var input=Files.newInputStream(path)){byte[] b=new byte[65536];int n;while((n=input.read(b))!=-1)digest.update(b,0,n);}
  return HexFormat.of().formatHex(digest.digest());
 }
 static void require(boolean value,String message){if(!value)throw new IllegalStateException(message);}
 static String verify(Path directory,Frozen frozen,String sourceSha,ObjectMapper json)throws Exception{
  Path manifest=directory.resolve("manifest.json"),receipt=directory.resolve("preparation.json");
  require(Files.isRegularFile(manifest,LinkOption.NOFOLLOW_LINKS)&&Files.size(manifest)<=1048576,"执行副本清单缺失或超限");
  require(manifest.toRealPath().equals(manifest.toAbsolutePath().normalize()),"执行副本不能包含符号链接");
  String fingerprint=sha(manifest);
  JsonNode node=json.readTree(manifest.toFile()),proof=json.readTree(receipt.toFile());
  require(proof.path("verified").asBoolean()&&fingerprint.equals(proof.path("manifestSha256").asText()),"执行副本准备回执与清单不一致");
  require(node.path("version").asInt()==1&&"csv-v2".equals(node.path("semanticsVersion").asText())&&
   frozen.format().equals(node.path("format").asText())&&sourceSha.equals(node.path("sourceSha256").asText())&&
   node.path("rowCount").isIntegralNumber()&&node.path("rowCount").asLong(-1)==frozen.rowCount()&&
   node.path("columns").equals(json.valueToTree(frozen.columns())),"执行副本元数据与冻结快照不一致");
  var files=node.path("files");require(files.isArray()&&files.size()>0&&files.size()<=256,"执行副本文件列表不合法");
  var seen=new HashSet<String>();long size=0;
  for(JsonNode file:files){
   String name=file.path("path").asText(),suffix=frozen.format().equals("CSV")?".csv":".parquet";
   require(name.matches("data/part-[A-Za-z0-9_.-]+")&&name.endsWith(suffix)&&seen.add(name),"执行副本路径不合法");
   Path path=directory.resolve(name);
   require(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)&&path.toRealPath().equals(path.toAbsolutePath().normalize()),"执行副本文件缺失或包含链接");
   size=Math.addExact(size,Files.size(path));require(size<=256L*1024*1024,"执行副本文件总量超限");
   require(file.path("size").isIntegralNumber()&&file.path("size").asLong(-1)==Files.size(path)&&sha(path).equals(file.path("sha256").asText()),"执行副本文件指纹不一致");
  }
  return fingerprint;
 }
}
