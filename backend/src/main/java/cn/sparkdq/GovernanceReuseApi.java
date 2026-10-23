package cn.sparkdq;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController @RequestMapping("/api/governance-policies")
public class GovernanceReuseApi {
 final Pages pages;final GovernanceReuse reuse;
 public GovernanceReuseApi(Pages pages,GovernanceReuse reuse){this.pages=pages;this.reuse=reuse;}
 @GetMapping public Pages.Page list(@RequestParam(value="page",required=false) String page,@RequestParam(value="pageSize",required=false) String size,@RequestParam(value="q",required=false) String query){return pages.reusablePolicies(page,size,query);}
 @GetMapping("/{sourceId}") public Map<String,Object> prepare(@PathVariable("sourceId") String sourceId,@RequestParam("targetTaskId") String targetId){return reuse.prepare(sourceId,targetId);}
}
