package cn.sparkdq;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController
public class MysqlImportApi {
 final MysqlImports imports;final MysqlSources sources;
 public MysqlImportApi(MysqlImports imports,MysqlSources sources){this.imports=imports;this.sources=sources;}
 @GetMapping("/api/mysql-sources") public List<Map<String,Object>> sources(){return sources.list();}
 @PostMapping("/api/mysql-imports") @ResponseStatus(org.springframework.http.HttpStatus.ACCEPTED)
 public Map<String,Object> submit(@RequestBody MysqlImports.Request request,@RequestHeader(value="X-Import-Key",required=false) String key)throws Exception {
  try{return imports.submit(request,key);}catch(IllegalArgumentException error){throw Datasets.bad("字段选择或业务源配置不符合契约");}
 }
 @GetMapping("/api/mysql-imports/{id}") public Map<String,Object> detail(@PathVariable("id") String id){return imports.detail(id);}
 @GetMapping("/api/mysql-imports/page") public Pages.Page page(@RequestParam(value="page",defaultValue="1") String page,@RequestParam(value="pageSize",defaultValue="20") String size){return imports.page(page,size);}
 @PostMapping("/api/mysql-imports/{id}/cancel") public Map<String,Object> cancel(@PathVariable("id") String id){return imports.cancel(id);}
}
