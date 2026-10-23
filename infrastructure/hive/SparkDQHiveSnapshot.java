import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.*;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Admin-only, bounded fixture export through an actual Hive Catalog. */
public class SparkDQHiveSnapshot {
 public static void main(String[] args)throws Exception {
  Path root=Path.of("/lab");String mode=args[0];var json=new ObjectMapper();
  var spark=SparkSession.builder().appName("SparkDQ-Hive-"+mode).master("local[2]")
   .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","2")
   .config("spark.sql.warehouse.dir","file:/lab/warehouse")
   .config("spark.hadoop.javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName=/lab/metastore_db;create=true")
   .config("spark.sql.hive.metastore.version","2.3.9").config("spark.sql.hive.metastore.jars","builtin").enableHiveSupport().getOrCreate();
  try {
   if(!spark.version().equals("3.5.8")||!spark.conf().get("spark.sql.catalogImplementation").equals("hive"))throw new IllegalStateException("Catalog/version mismatch");
   if(mode.equals("create")){
    String ddl="CREATE TABLE sparkdq_lab.quality_fixture (id STRING,n STRING,x STRING,y STRING,note STRING) STORED AS PARQUET";
    spark.sql("CREATE DATABASE sparkdq_lab");spark.sql(ddl);
    // Explicit schema and CSV contract; CRLF including quoted content retained.
    spark.read().option("header","true").option("multiLine","true").option("lineSep","\r\n").option("nullValue","\\N").option("emptyValue", "").option("escape","\"").schema("id STRING,n STRING,x STRING,y STRING,note STRING").csv("/lab/reference.csv").createOrReplaceTempView("fixture_input");
    spark.sql("INSERT INTO sparkdq_lab.quality_fixture SELECT id,n,x,y,note FROM fixture_input");
    Files.writeString(root.resolve("ddl.sql"),"CREATE DATABASE sparkdq_lab;\n"+ddl+";\nINSERT INTO sparkdq_lab.quality_fixture SELECT id,n,x,y,note FROM fixture_input;\n");
    if(spark.table("sparkdq_lab.quality_fixture").count()!=14)throw new IllegalStateException("Row count mismatch");
   } else if(mode.equals("export")){
    var table=spark.table("sparkdq_lab.quality_fixture");var info=new LinkedHashMap<String,String>();
    for(Row row:spark.sql("DESCRIBE EXTENDED sparkdq_lab.quality_fixture").collectAsList())if(!row.isNullAt(0)&&!row.isNullAt(1))info.put(row.getString(0).strip(),row.getString(1));
    if(!"hive".equals(info.get("Provider"))||!info.get("Serde Library").contains("ParquetHiveSerDe")||!info.get("Location").startsWith("file:/lab/warehouse/"))throw new IllegalStateException("Not a Hive SerDe table");
    var rows=table.limit(101).collectAsList();if(rows.size()!=14||rows.size()>100)throw new IllegalStateException("Bounded export refused");
    try(var out=Files.newBufferedWriter(root.resolve("hive.csv"),StandardOpenOption.CREATE_NEW)){
     out.write(String.join(",",table.columns())+"\r\n");
     for(Row row:rows){for(int i=0;i<row.size();i++){if(i>0)out.write(',');out.write(row.isNullAt(i)?"\\N":"\""+row.getString(i).replace("\"","\"\"")+"\"");}out.write("\r\n");}
    }
    String sha=HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(root.resolve("hive.csv"))));
    var receipt=Map.of("version",1,"catalog","HIVE","database","sparkdq_lab","table","quality_fixture","provider","hive","metastore","EMBEDDED_DERBY","sparkVersion",spark.version(),"rows",rows.size(),"columns",List.of(table.columns()),"canonicalSha256",sha);
    json.writerWithDefaultPrettyPrinter().writeValue(root.resolve("hive-receipt.json").toFile(),receipt);
    json.writerWithDefaultPrettyPrinter().writeValue(root.resolve("catalog-evidence.json").toFile(),Map.of("ok",true,"independentProcessRead",true,"describeExtended",info,"receipt",receipt));
   }else throw new IllegalArgumentException("create or export required");
  }finally{spark.stop();}
 }
}
