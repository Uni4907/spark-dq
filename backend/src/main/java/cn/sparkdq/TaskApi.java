package cn.sparkdq;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController @RequestMapping("/api/tasks")
public class TaskApi {
 final Tasks tasks;final Reports reports;final ReportDocuments documents;public TaskApi(Tasks tasks,Reports reports,ReportDocuments documents){this.tasks=tasks;this.reports=reports;this.documents=documents;}
 @PostMapping public Map<String,Object> create(@RequestBody Tasks.Request request){return tasks.create(request);}
 @PostMapping("/{id}/cancel") public Map<String,Object> cancel(@PathVariable("id") String id){return tasks.cancel(id);}
 @PostMapping("/{id}/rerun") public Map<String,Object> rerun(@PathVariable("id") String id,@RequestBody(required=false) Tasks.RerunRequest request){return tasks.rerun(id,request);}
 @PostMapping("/{id}/govern") public Map<String,Object> govern(@PathVariable("id") String id,@RequestBody Tasks.GovernanceRequest request){return tasks.govern(id,request);}
 @GetMapping("/{id}/report") public org.springframework.http.ResponseEntity<Map<String,Object>> report(@PathVariable("id") String id){
  return org.springframework.http.ResponseEntity.ok().header("Content-Disposition","attachment; filename=quality-report.json").body(reports.report(id));
 }
 @GetMapping(value="/{id}/report.html",produces="text/html;charset=UTF-8")
 public org.springframework.http.ResponseEntity<String> document(@PathVariable("id") String id,@RequestParam(name="download",defaultValue="false") boolean download){
  return org.springframework.http.ResponseEntity.ok().header("Content-Disposition",(download?"attachment":"inline")+"; filename=quality-report.html")
   .header("Cache-Control","no-store").header("X-Content-Type-Options","nosniff")
   .header("Content-Security-Policy","default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
   .contentType(org.springframework.http.MediaType.parseMediaType("text/html;charset=UTF-8")).body(documents.document(id));
 }
 @GetMapping public List<Map<String,Object>> list(){return tasks.list();}
 @GetMapping("/{id}") public Map<String,Object> detail(@PathVariable("id") String id){return tasks.detail(id);}
 @GetMapping("/{id}/rules/{rule}/preview") public List<Object> preview(@PathVariable("id") String id,@PathVariable("rule") String rule)throws Exception{return tasks.preview(id,rule);}
}
