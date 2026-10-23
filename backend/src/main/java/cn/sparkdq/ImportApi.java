package cn.sparkdq;
import java.util.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
@RestController @RequestMapping("/api/imports")
public class ImportApi {
 final NativeImports imports;
 public ImportApi(NativeImports imports){this.imports=imports;}
 @PostMapping @ResponseStatus(org.springframework.http.HttpStatus.ACCEPTED)
 public Map<String,Object> upload(@RequestParam("file") MultipartFile file,@RequestHeader(value="X-Import-Key",required=false) String key)throws Exception{return imports.upload(file,key);}
 @GetMapping("/page") public Pages.Page page(@RequestParam(value="page",defaultValue="1") String page,@RequestParam(value="pageSize",defaultValue="20") String pageSize){return imports.page(page,pageSize);}
 @GetMapping("/{id}") public Map<String,Object> detail(@PathVariable("id") String id){return imports.detail(id);}
 @PostMapping("/{id}/cancel") public Map<String,Object> cancel(@PathVariable("id") String id){return imports.cancel(id);}
 @PostMapping("/{id}/retry") @ResponseStatus(org.springframework.http.HttpStatus.ACCEPTED)
 public Map<String,Object> retry(@PathVariable("id") String id,@RequestHeader(value="X-Import-Key",required=false) String key)throws Exception{return imports.retry(id,key);}
}
