package cn.sparkdq;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class GovernancePoliciesTest {
 final List<String> columns=List.of("name","age");
 Tasks.GovernanceRequest policy(Map<String,Object> fills,Map<String,Object> normalizations){return new Tasks.GovernanceRequest(false,false,fills,normalizations,null);}
 @Test void canonicalizesAndFreezesIndependentMaps(){
  var fills=new HashMap<String,Object>(Map.of("name","Unknown"));var norms=new HashMap<String,Object>(Map.of("name","TRIM"));
  var normalized=GovernancePolicies.normalize(policy(fills,norms),columns);
  fills.put("name","changed");norms.put("name","UPPER");
  assertEquals(Map.of("name","Unknown"),normalized.fills());assertEquals(Map.of("name","TRIM"),normalized.normalizations());
  assertEquals(2,normalized.policyVersion());assertThrows(UnsupportedOperationException.class,()->normalized.fills().put("age","0"));
 }
 @Test void rejectsBlankReservedOversizedAndNonStringFills(){
  for(Object invalid:List.of("","   ","\\N","x".repeat(1001),"\u0000",1,true)){
   assertThrows(ResponseStatusException.class,()->GovernancePolicies.normalize(policy(Map.of("name",invalid),Map.of()),columns));
  }
  var nullValue=new HashMap<String,Object>();nullValue.put("name",null);
  assertThrows(ResponseStatusException.class,()->GovernancePolicies.normalize(policy(nullValue,Map.of()),columns));
 }
 @Test void rejectsUnknownColumnsOperationsEmptyPolicyAndVersion(){
  assertThrows(ResponseStatusException.class,()->GovernancePolicies.normalize(policy(Map.of("other","x"),Map.of()),columns));
  assertThrows(ResponseStatusException.class,()->GovernancePolicies.normalize(policy(Map.of(),Map.of("name","NONE")),columns));
  assertThrows(ResponseStatusException.class,()->GovernancePolicies.normalize(policy(Map.of(),Map.of()),columns));
  assertThrows(ResponseStatusException.class,()->GovernancePolicies.normalize(new Tasks.GovernanceRequest(true,false,Map.of(),Map.of(),1),columns));
 }
 @Test void oldTwoBooleanRequestsRemainSupported(){
  var p=GovernancePolicies.normalize(new Tasks.GovernanceRequest(true,true),columns);
  assertTrue(p.dropMissing());assertTrue(p.deduplicate());assertTrue(p.fills().isEmpty());assertTrue(p.normalizations().isEmpty());
 }
 @Test void preservesLiteralCsvUnicodeAndBackslashValues(){
  String literal="  中文, \"quoted\"\nC:\\work\\files  ";
  assertEquals(literal,GovernancePolicies.normalize(policy(Map.of("name",literal),Map.of("name","TRIM_UPPER")),columns).fills().get("name"));
 }
}
