package cn.sparkdq;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class ReportsTest {
 Map<String,Object> rule(String type,List<String> columns,String metric,long count){return Map.of("id",UUID.randomUUID().toString(),"type",type,"columns",columns,"metrics",Map.of(metric,count));}
 @Test void matchesRulesByDefinitionAndPreservesWorsening(){
  var before=List.of(rule("MISSING",List.of("name"),"blankCount",2),rule("DUPLICATE",List.of("id"),"duplicateRows",5));
  var after=List.of(rule("DUPLICATE",List.of("id"),"duplicateRows",0),rule("MISSING",List.of("name"),"blankCount",3));
  var compared=Reports.compare(before,after);
  assertEquals(2,compared.size());assertEquals(5L,compared.get(0).get("reducedBy"));assertEquals(-1L,compared.get(1).get("reducedBy"));
 }
 @Test void unmatchedFieldsAreNotCompared(){
  assertTrue(Reports.compare(List.of(rule("MISSING",List.of("name"),"blankCount",2)),List.of(rule("MISSING",List.of("email"),"blankCount",0))).isEmpty());
 }
 @Test void differentParametersCannotBeComparedAsTheSameRule(){
  var a=new HashMap<>(rule("TYPE",List.of("value"),"invalidCount",2));a.put("params",Map.of("dataType","INTEGER"));
  var b=new HashMap<>(rule("TYPE",List.of("value"),"invalidCount",0));b.put("params",Map.of("dataType","DECIMAL"));
  assertTrue(Reports.compare(List.of(a),List.of(b)).isEmpty());
 }
}
