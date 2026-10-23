package cn.sparkdq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import java.nio.file.*;
import java.util.*;

/** Server-owned, immutable presets. The HTTP request contains only an alias. */
@Service
public class ControlledFiles {
 final Datasets datasets;final Map<String,Source> sources;
 record Source(String id,String name,Path path,String sha256,Map<String,Object> hive){}
 public ControlledFiles(Datasets datasets,ObjectMapper json){this.datasets=datasets;sources=load(json,System.getenv("SPARK_DQ_CONTROLLED_FILES_FILE"));}
 static Map<String,Source> load(ObjectMapper json,String filename){
  if(filename==null||filename.isBlank())return Map.of();
  try{
   Path config=Path.of(filename);if(!config.isAbsolute()||config.normalize().startsWith(Datasets.ROOT)||!Files.isRegularFile(config,LinkOption.NOFOLLOW_LINKS)||!config.toRealPath().equals(config.normalize())||Files.size(config)>65536)throw new IllegalArgumentException();
   var node=json.readTree(config.toFile());if(!node.isObject()||node.size()!=1||!node.path("files").isArray()||node.path("files").size()>16)throw new IllegalArgumentException();
   var result=new TreeMap<String,Source>();
   for(var item:node.path("files")){
    if(!item.isObject()||(item.size()!=4&&item.size()!=5))throw new IllegalArgumentException();var keys=item.fieldNames();while(keys.hasNext())if(!Set.of("id","name","path","sha256","hiveReceipt").contains(keys.next()))throw new IllegalArgumentException();
    String id=item.path("id").asText(),name=item.path("name").asText(),sha=item.path("sha256").asText();Path path=Path.of(item.path("path").asText());
    if(!id.matches("[A-Za-z][A-Za-z0-9_-]{0,63}")||!name.matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,195}\\.csv")||!sha.matches("[a-f0-9]{64}")||!path.isAbsolute()||!path.normalize().startsWith(Path.of("/run/controlled-inputs"))||!path.toRealPath().equals(path.normalize())||!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)||Files.isWritable(path)||Files.size(path)>ControlledProfile.MAX_BYTES)throw new IllegalArgumentException();
    if(result.putIfAbsent(id,new Source(id,name,path,sha,item.has("hiveReceipt")?HiveReceipt.load(json,Path.of(item.path("hiveReceipt").asText()),sha):Map.of()))!=null)throw new IllegalArgumentException();
   }
   return Map.copyOf(result);
  }catch(Exception error){throw new IllegalStateException("CONTROLLED_FILES_CONFIGURATION_INVALID");}
 }
 public List<Map<String,Object>> list(){return sources.values().stream().sorted(Comparator.comparing(Source::id)).map(s->Map.<String,Object>of("id",s.id(),"name",s.name(),"sha256",s.sha256(),"maxBytes",ControlledProfile.MAX_BYTES,"maxRows",ControlledProfile.MAX_ROWS)).toList();}
 public Map<String,Object> register(String id)throws Exception{
  var source=sources.get(id);if(source==null)throw Datasets.bad("受控文件未配置或不在白名单");
  var provenance=new LinkedHashMap<String,Object>();provenance.put("kind","PRESET");provenance.put("sourceId",source.id());if(!source.hive().isEmpty())provenance.put("hive",source.hive());
  return datasets.registerControlled(source.name(),source.path(),false,provenance,source.sha256());
 }
}
@RestController
class ControlledFilesApi {
 final ControlledFiles files;ControlledFilesApi(ControlledFiles files){this.files=files;}
 @GetMapping("/api/controlled-files") public List<Map<String,Object>> list(){return files.list();}
 @PostMapping("/api/controlled-files/{id}/register") public Map<String,Object> register(@PathVariable("id")String id)throws Exception{return files.register(id);}
}
