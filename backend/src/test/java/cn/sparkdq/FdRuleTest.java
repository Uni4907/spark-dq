package cn.sparkdq;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
class FdRuleTest {
 Rules.Spec rule(List<String> x,Object y){return new Rules.Spec("FD",x,Map.of("dependentColumns",y));}
 @Test void validatesCompositeTuplesAndFreezesDependentList(){
  var y=new ArrayList<>(List.of("y1","y2"));var definitions=Rules.normalize(List.of(rule(List.of("x1","x2"),y)),List.of("x1","x2","y1","y2"));
  y.set(0,"changed");assertEquals(Map.of("dependentColumns",List.of("y1","y2")),definitions.get(0).get("params"));
  assertEquals(definitions,Rules.normalize(Rules.stored(definitions),List.of("x1","x2","y1","y2")));
 }
 @Test void rejectsEmptyDuplicateOverlapMissingAndWrongTypes(){
  for(Object y:List.of(List.of(),List.of("y","y"),List.of("x"),List.of("missing"),List.of(42),"y"))assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(rule(List.of("x"),y)),List.of("x","y")));
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(rule(List.of(),List.of("y"))),null));
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(new Rules.Spec("FD",List.of("x"),Map.of("dependentColumns",List.of("y"),"nullPolicy","VALUE"))),null));
 }
 @Test void countLimitsCaseAndReservedSqlNames(){
  assertDoesNotThrow(()->Rules.normalize(List.of(rule(List.of("select","Case"),List.of("case"))),List.of("select","Case","case")));
  var tooMany=java.util.stream.IntStream.range(0,17).mapToObj(i->"x"+i).toList();
  assertThrows(ResponseStatusException.class,()->Rules.normalize(List.of(rule(tooMany,List.of("y"))),null));
 }
 @Test void conflictParticipantsUseComparableInvalidCount(){
  var a=Map.<String,Object>of("type","FD","columns",List.of("x"),"params",Map.of("dependentColumns",List.of("y")),"metrics",Map.of("invalidCount",3,"conflictRows",3));
  var b=new HashMap<>(a);b.put("metrics",Map.of("invalidCount",2,"conflictRows",2));
  assertEquals(1L,Reports.compare(List.of(a),List.of(b)).get(0).get("reducedBy"));
 }
}
