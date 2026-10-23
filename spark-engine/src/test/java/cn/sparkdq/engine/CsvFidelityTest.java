package cn.sparkdq.engine;
import org.junit.jupiter.api.Test;
import org.apache.spark.sql.SparkSession;
import java.nio.file.Files;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
class CsvFidelityTest {
 static String q(String value){return "\""+value.replace("\"","\"\"")+"\"";}
 @Test void sparkWrittenPartsCanBeRecheckedWithoutChangingQuotedValues()throws Exception{
  var spark=SparkSession.builder().master("local[1]").appName("csv-directory-fidelity")
   .config("spark.ui.enabled","false").config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1").getOrCreate();
  var root=Files.createTempDirectory("dq-csv-parts-");
  try{
   var input=root.resolve("source.csv");String text="中文\r\n第二行\n引号\"\t";
   Files.writeString(input,q("id")+","+q("text")+"\r\n"+q("1")+","+q(text)+"\r\n"+q("2")+","+q("")+"\r\n"+q("3")+","+q("\\N")+"\r\n");
   var original=Engine.readCsv(spark,input.toString(),true);var parts=root.resolve("governed");
   original.repartition(2).write().option("header","true").option("nullValue","\\N").option("emptyValue","\"\"").option("escape","\"").option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace","false").csv(parts.toString());
   Files.writeString(parts.resolve("_ignored"),"not csv\r");Files.writeString(parts.resolve(".ignored"),"not csv\r");
   assertEquals(original.orderBy("id").collectAsList(),Engine.readCsv(spark,parts.toString(),true).orderBy("id").collectAsList());
  }finally{spark.stop();try(var files=Files.walk(root)){for(var path:files.sorted(java.util.Comparator.reverseOrder()).toList())Files.delete(path);}}
 }
 @Test void ambiguousOrEmptyCsvDirectoriesAreRejected()throws Exception{
  var root=Files.createTempDirectory("dq-csv-invalid-parts-");
  try{
   assertThrows(IllegalArgumentException.class,()->Engine.recordSeparator(root.toString()));
   Files.writeString(root.resolve("part-1.csv"),"id\n1\n");Files.writeString(root.resolve("part-2.csv"),"id\r\n2\r\n");
   assertThrows(IllegalArgumentException.class,()->Engine.recordSeparator(root.toString()));
  }finally{try(var files=Files.walk(root)){for(var path:files.sorted(java.util.Comparator.reverseOrder()).toList())Files.delete(path);}}
 }
 @Test void quotedTextKeepsAllLineEndingsForEachRecordSeparator()throws Exception{
  var spark=SparkSession.builder().master("local[1]").appName("csv-fidelity")
   .config("spark.ui.enabled","false").config("spark.driver.host","127.0.0.1")
   .config("spark.driver.bindAddress","127.0.0.1").getOrCreate();
  try{
   for(String sep:List.of("\n","\r\n","\r")){
    var path=Files.createTempFile("dq-csv-fidelity-",".csv");
    try{
     String value="中文\r\n第二行\n第三行\r制表\t引号\"";
     Files.writeString(path,q("id")+","+q("text")+sep+q("1")+","+q(value)+sep+q("2")+","+q("")+sep+q("3")+","+q("\\N")+sep);
     var rows=Engine.readCsv(spark,path.toString(),true).collectAsList();
     assertEquals(3,rows.size());assertEquals("中文\r\n第二行\n第三行\r制表\t引号\"",rows.get(0).getString(1));
     assertEquals("",rows.get(1).getString(1));assertNull(rows.get(2).get(1));
    }finally{Files.deleteIfExists(path);}
   }
  }finally{spark.stop();}
 }
}
