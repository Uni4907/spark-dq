package cn.sparkdq;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.util.*;
@RestController
@RequestMapping("/api/datasets")
public class DatasetApi {
 private final Datasets service; private final CsvImports imports;
 public DatasetApi(Datasets service,CsvImports imports){this.service=service;this.imports=imports;}
 @PostMapping public Map<String,Object> upload(@RequestParam("file") MultipartFile file)throws Exception{return service.upload(file);}
 @PostMapping("/csv-parts") public Map<String,Object> uploadParts(@RequestParam("files") List<MultipartFile> files)throws Exception{return imports.upload(files);}
 @GetMapping public List<Map<String,Object>> list(){return service.db.queryForList("select id,name,size_bytes,row_count,created_at from datasets order by created_at desc,id desc limit 200");}
 @GetMapping("/{id}/download") public org.springframework.http.ResponseEntity<org.springframework.core.io.Resource> download(@PathVariable("id") String id){
  var data=service.one(id);
  return org.springframework.http.ResponseEntity.ok().header("Content-Disposition","attachment; filename=dataset-"+data.get("id")+".csv")
   .contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM).body(new org.springframework.core.io.FileSystemResource(data.get("file_path").toString()));
 }
 @GetMapping("/{id}/sources/{ordinal}/download") public org.springframework.http.ResponseEntity<org.springframework.core.io.Resource> source(@PathVariable("id") String id,@PathVariable("ordinal") int ordinal)throws Exception{
  var source=service.sourceFile(id,ordinal);
  return org.springframework.http.ResponseEntity.ok().header("Content-Disposition","attachment; filename=source-"+ordinal+(source.toString().endsWith(".parquet")?".parquet":".csv")).contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM).body(new org.springframework.core.io.FileSystemResource(source));
 }
 @GetMapping("/{id}") public Map<String,Object> one(@PathVariable("id") String id){return service.one(id);}
}
