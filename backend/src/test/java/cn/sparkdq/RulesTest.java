package cn.sparkdq;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class RulesTest {
 Rules.Spec spec(String type,Map<String,Object> params){return new Rules.Spec(type,List.of("value"),params);}
 @Test void rejectsInvalidTypesColumnsAndVersion(){
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(spec("UNKNOWN",Map.of())),null));
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(spec("MISSING",Map.of())),List.of("other")));
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(new Rules.Spec("TYPE",List.of("value"),Map.of("dataType","INTEGER"),2)),null));
 }
 @Test void rejectsNullTypeParameterAsBadRequest(){
  var p=new HashMap<String,Object>();p.put("dataType",null);
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(spec("TYPE",p)),null));
 }
 @Test void validatesPatternAndDuplicateDefinitions(){
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(spec("REGEX",Map.of("pattern","["))),null));
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(spec("MISSING",Map.of()),spec("MISSING",Map.of())),null));
 }
 @Test void canonicalizesExactRangeWithoutDoubleRounding(){
  var rule=Rules.normalize(List.of(spec("RANGE",Map.of("min","0.1000000000","max","9999999999999999999999999999.1234567890"))),null).get(0);
  assertEquals(Map.of("min","0.1","max","9999999999999999999999999999.123456789"),rule.get("params"));
 }
 @Test void rejectsInvertedAndOverprecisionRanges(){
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(spec("RANGE",Map.of("min","2","max","1"))),null));
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(spec("RANGE",Map.of("max","0.00000000001"))),null));
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(spec("RANGE",Map.of("min","NaN"))),null));
 }
 @Test void snapshotsDoNotKeepMutableCallerListsOrMaps(){
  var columns=new ArrayList<>(List.of("value"));var params=new HashMap<String,Object>(Map.of("dataType","INTEGER"));
  var normalized=Rules.normalize(List.of(new Rules.Spec("TYPE",columns,params)),null);
  columns.set(0,"changed");params.put("dataType","DATE");
  assertEquals(List.of("value"),normalized.get(0).get("columns"));
  assertEquals(Map.of("dataType","INTEGER"),normalized.get(0).get("params"));
  var first=Rules.withIds(normalized);var second=Rules.withIds(normalized);
  assertNotEquals(first.get(0).get("id"),second.get(0).get("id"));
 }
}
