package cn.sparkdq;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.type.TypeReference;
import java.nio.file.*;
import java.util.*;

/** Receipt supplied by an administrator's bounded Hive Catalog export, never by HTTP. */
final class HiveReceipt {
 static Map<String,Object> load(ObjectMapper json,Path path,String sha)throws Exception{
  if(!path.isAbsolute()||!path.normalize().startsWith(Path.of("/run/controlled-inputs"))||!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)||!path.toRealPath().equals(path.normalize())||Files.isWritable(path)||Files.size(path)>16384)throw new IllegalArgumentException();
  var node=json.readTree(path.toFile());validate(node,sha);
  Map<String,Object> value=json.convertValue(node,new TypeReference<LinkedHashMap<String,Object>>(){});value.put("receiptSha256",ExecutionInputs.sha(path));return Map.copyOf(value);
 }
 static void validate(JsonNode node,String sha){
  if(!node.isObject()||node.size()!=10||!node.path("version").isInt()||node.path("version").asInt()!=1||!"HIVE".equals(node.path("catalog").asText())||!"hive".equals(node.path("provider").asText())||!"EMBEDDED_DERBY".equals(node.path("metastore").asText())||!"3.5.8".equals(node.path("sparkVersion").asText())||!sha.equals(node.path("canonicalSha256").asText()))throw new IllegalArgumentException();
  for(String field:List.of("database","table"))if(!node.path(field).asText().matches("[A-Za-z][A-Za-z0-9_]{0,63}"))throw new IllegalArgumentException();
  if(!node.path("rows").isIntegralNumber()||node.path("rows").asLong()<1||node.path("rows").asLong()>100)throw new IllegalArgumentException();
  var columns=node.path("columns");if(!columns.isArray()||columns.isEmpty()||columns.size()>100)throw new IllegalArgumentException();var seen=new HashSet<String>();
  for(var column:columns)if(!column.isTextual()||!column.asText().matches("[A-Za-z][A-Za-z0-9_]{0,63}")||!seen.add(column.asText()))throw new IllegalArgumentException();
 }
}
