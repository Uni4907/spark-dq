package cn.sparkdq;
import java.io.IOException;
import java.util.Map;
import org.springframework.web.bind.annotation.*;
@RestController @RequestMapping("/api/internal/maintenance")
public class MaintenanceApi {
 final Maintenance maintenance;
 public MaintenanceApi(Maintenance maintenance){this.maintenance=maintenance;}
 public record Request(String owner){}
 @GetMapping public Map<String,Object> state(@RequestHeader(name="X-SparkDQ-Maintenance-Key",required=false) String key) {
  maintenance.authorize(key);return maintenance.state();
 }
 @PostMapping("/enter") public Map<String,Object> enter(@RequestHeader(name="X-SparkDQ-Maintenance-Key",required=false) String key,@RequestBody Request request)throws IOException {
  maintenance.authorize(key);return maintenance.enter(request.owner());
 }
 @PostMapping("/leave") public Map<String,Object> leave(@RequestHeader(name="X-SparkDQ-Maintenance-Key",required=false) String key,@RequestBody Request request)throws IOException {
  maintenance.authorize(key);return maintenance.leave(request.owner());
 }
}
