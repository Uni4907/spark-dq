package cn.sparkdq;
import org.springframework.web.bind.annotation.*;

/** Separate routes preserve the original array endpoints for existing clients. */
@RestController
public class PagingApi {
 final Pages pages;
 public PagingApi(Pages pages){this.pages=pages;}
 @GetMapping("/api/tasks/page")
 public Pages.Page tasks(@RequestParam(value="page",required=false) String page,
  @RequestParam(value="pageSize",required=false) String size,@RequestParam(value="status",required=false) String status){
  return pages.tasks(page,size,status);
 }
 @GetMapping("/api/datasets/{id}/tasks")
 public Pages.Page datasetTasks(@PathVariable("id") String id,@RequestParam(value="page",required=false) String page,
  @RequestParam(value="pageSize",required=false) String size,@RequestParam(value="status",required=false) String status){
  return pages.datasetTasks(id,page,size,status);
 }
 @GetMapping("/api/datasets/page")
 public Pages.Page datasets(@RequestParam(value="page",required=false) String page,
  @RequestParam(value="pageSize",required=false) String size,@RequestParam(value="q",required=false) String query){
  return pages.datasets(page,size,query);
 }
}
