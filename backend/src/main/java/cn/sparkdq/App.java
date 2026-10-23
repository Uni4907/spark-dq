package cn.sparkdq;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.Map;
@SpringBootApplication
@RestController
public class App {
 private final JdbcTemplate db;private final Maintenance maintenance;
 public App(JdbcTemplate db,Maintenance maintenance){this.db=db;this.maintenance=maintenance;}
 public static void main(String[] args){SpringApplication.run(App.class,args);}
 @GetMapping("/api/health") public Map<String,Object> health(){return Map.of("maintenance",maintenance.enabled(),"status","UP","database",db.queryForObject("select version()",String.class),"schemaVersion",db.queryForObject("select version from flyway_schema_history where success=1 order by installed_rank desc limit 1",String.class));}
}
