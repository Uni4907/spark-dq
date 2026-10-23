package cn.sparkdq;

import com.fasterxml.jackson.databind.*;
import java.nio.file.*;
import java.util.*;

/** Independently binds the worker manifest to frozen source and canonical bytes. */
final class NativeProof {
 static final long LIMIT=20L*1024*1024;
 static final Map<String,Object> CONVERSION=Map.of("version",1,"timezone","UTC","decimal","PLAIN_WITH_SCALE","integer","BASE10","boolean","LOWERCASE","date","GREGORIAN_1582_10_15_TO_9999","string","STRICT_UTF8","order","UNSPECIFIED");
 record Verified(String manifestSha,String canonicalSha,long rows,List<String> columns,JsonNode manifest){}
 static Path file(Path path,long min,long max)throws Exception {
  path=path.toAbsolutePath().normalize();
  if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)||!path.toRealPath().equals(path)||Files.size(path)<min||Files.size(path)>max)throw new IllegalStateException("导入文件缺失、链接或大小超限");
  return path;
 }
 static Verified verify(Path root,String sourceSha,long sourceBytes,ObjectMapper json)throws Exception {
  Path source=file(root.resolve("source.parquet"),12,LIMIT),manifest=file(root.resolve("prepared/manifest.json"),2,1048576),csv=file(root.resolve("prepared/canonical.csv"),1,LIMIT);
  if(Files.size(source)!=sourceBytes||!ExecutionInputs.sha(source).equals(sourceSha))throw new IllegalStateException("原文件指纹不匹配");
  JsonNode node=json.readTree(manifest.toFile());
  if(!node.path("version").isIntegralNumber()||node.path("version").intValue()!=1||!"PARQUET_NATIVE".equals(node.path("format").asText())||!"csv-v2".equals(node.path("semanticsVersion").asText())||!node.path("verified").isBoolean()||!node.path("verified").booleanValue()||!sourceSha.equals(node.path("sourceSha256").asText())||!node.path("sourceBytes").isIntegralNumber()||node.path("sourceBytes").longValue()!=sourceBytes||!node.path("conversion").equals(json.valueToTree(CONVERSION)))throw new IllegalStateException("导入转换契约不匹配");
  if(!node.path("rowCount").isIntegralNumber()||node.path("rowCount").longValue()<1||node.path("rowCount").longValue()>1000000)throw new IllegalStateException("导入行数超限");
  var columns=new ArrayList<String>();
  if(!node.path("columns").isArray()||node.path("columns").isEmpty()||node.path("columns").size()>100)throw new IllegalStateException("导入字段不完整");
  for(JsonNode col:node.path("columns")) {
   String name=col.path("name").asText(),type=col.path("type").asText();
   boolean scalar=Set.of("string","boolean","tinyint","smallint","int","bigint","date").contains(type);
   var decimal=java.util.regex.Pattern.compile("decimal\\((\\d{1,2}),(\\d{1,2})\\)").matcher(type);
   if(decimal.matches()){int p=Integer.parseInt(decimal.group(1)),s=Integer.parseInt(decimal.group(2));scalar=p>=1&&p<=38&&s<=p;}
   if(!name.matches("[A-Za-z][A-Za-z0-9_]{0,63}")||columns.contains(name)||!scalar||!col.path("nullable").isBoolean())throw new IllegalStateException("导入字段契约不匹配");
   columns.add(name);
  }
  JsonNode canonical=node.path("canonical");String sha=ExecutionInputs.sha(csv);
  if(!"canonical.csv".equals(canonical.path("path").asText())||!sha.equals(canonical.path("sha256").asText())||!canonical.path("size").isIntegralNumber()||Files.size(csv)!=canonical.path("size").longValue())throw new IllegalStateException("转换结果指纹不匹配");
  return new Verified(ExecutionInputs.sha(manifest),sha,node.path("rowCount").longValue(),List.copyOf(columns),node);
 }
}
