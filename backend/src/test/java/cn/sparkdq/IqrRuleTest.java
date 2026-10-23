package cn.sparkdq;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
class IqrRuleTest {
 @Test void defaultAndExactCanonicalK(){
  var rule=Rules.normalize(List.of(new Rules.Spec("IQR",List.of("v"),Map.of())),List.of("v")).get(0);
  assertEquals(Map.of("k","1.5"),rule.get("params"));
  assertEquals(Map.of("k","0.0001"),Rules.normalize(List.of(new Rules.Spec("IQR",List.of("v"),Map.of("k","0.0001"))),null).get(0).get("params"));
  assertEquals(rule,Rules.normalize(Rules.stored(List.of(rule)),List.of("v")).get(0));
 }
 @Test void rejectsInvalidMultiplierAndSchema(){
  for(String k:List.of("0","-1","10.0001","0.00001","NaN","1e0"," 1.5"))assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(new Rules.Spec("IQR",List.of("v"),Map.of("k",k))),null));
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(new Rules.Spec("IQR",List.of("v","w"),Map.of())),null));
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(new Rules.Spec("IQR",List.of("v"),Map.of("min","1"))),null));
 }
 @Test void reportComparisonPreservesDataDependentMetrics(){
  var before=Map.<String,Object>of("type","IQR","columns",List.of("v"),"params",Map.of("k","1.5"),"metrics",Map.of("invalidCount",2,"q1","1","status","EVALUATED"));
  var after=new HashMap<>(before);after.put("metrics",Map.of("invalidCount",0,"q1","2","status","ZERO_IQR"));
  var comparison=Reports.compare(List.of(before),List.of(after)).get(0);
  assertEquals(2L,comparison.get("reducedBy"));assertEquals(before.get("metrics"),comparison.get("beforeMetrics"));
 }
}
