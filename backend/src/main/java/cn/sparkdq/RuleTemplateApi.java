package cn.sparkdq;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController @RequestMapping("/api/rule-templates")
public class RuleTemplateApi {
 final RuleTemplates templates;public RuleTemplateApi(RuleTemplates templates){this.templates=templates;}
 @GetMapping public List<Map<String,Object>> list(){return templates.list();}
 @PostMapping public Map<String,Object> create(@RequestBody RuleTemplates.Save request){return templates.create(request);}
 @GetMapping("/{id}") public Map<String,Object> one(@PathVariable("id") String id,@RequestParam(name="revision",required=false) Integer revision){return templates.one(id,revision);}
 @GetMapping("/{id}/versions") public List<Map<String,Object>> history(@PathVariable("id") String id){return templates.history(id);}
 @PutMapping("/{id}") public Map<String,Object> update(@PathVariable("id") String id,@RequestBody RuleTemplates.Save request){return templates.update(id,request);}
 @DeleteMapping("/{id}") public Map<String,Object> archive(@PathVariable("id") String id,@RequestParam(name="revision") int revision){return templates.archive(id,revision);}
}
