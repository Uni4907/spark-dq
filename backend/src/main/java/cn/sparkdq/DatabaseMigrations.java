package cn.sparkdq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

/** The only automatic baseline allowed is the complete, known pre-migration schema. */
@Configuration(proxyBeanMethods=false)
public class DatabaseMigrations {
 static String resource(String path) {
  try(var in=new ClassPathResource(path).getInputStream()) {
   return new String(in.readAllBytes(),StandardCharsets.UTF_8);
  } catch(IOException error) { throw new IllegalStateException("Missing migration resource: "+path,error); }
 }
 static void requireLegacy(List<String> actual) {
  var expected=resource("db/legacy-metadata.txt").lines().toList();
  if(!expected.equals(actual)) throw new IllegalStateException(
   "Legacy schema does not match the supported 12-table layout; automatic baseline refused. Preserve a backup and inspect schema differences.");
 }
 @Bean FlywayMigrationStrategy guardedMigrations() {
  return flyway -> {
   var db=new JdbcTemplate(flyway.getConfiguration().getDataSource());
   var tables=db.queryForList("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()",String.class);
   if(!tables.isEmpty()&&!tables.contains("flyway_schema_history")) {
    requireLegacy(db.queryForList(resource("db/legacy-metadata.sql"),String.class));
    if(!flyway.getConfiguration().getBaselineVersion().getVersion().equals("1"))
     throw new IllegalStateException("Legacy baseline must be version 1");
    flyway.baseline();
   }
   flyway.migrate(); // Validate checksums before task recovery; never repair or clean automatically.
  };
 }
}
