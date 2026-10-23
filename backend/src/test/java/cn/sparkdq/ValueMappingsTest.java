package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class ValueMappingsTest {
 List<String> columns=List.of("name","id","status","other","more");
 Tasks.GovernanceRequest request(Map<String,List<Tasks.ValueMapping>> mappings,Integer version){
  return new Tasks.GovernanceRequest(false,false,Map.of(),Map.of(),mappings,version);
 }
 @Test void canonicalizesAndDeepFreezesPolicy(){
  var pairs=new ArrayList<>(List.of(new Tasks.ValueMapping("B","C"),new Tasks.ValueMapping("A","B")));
  var maps=new HashMap<String,List<Tasks.ValueMapping>>();maps.put("name",pairs);
  var p=GovernancePolicies.normalize(request(maps,null),columns);
  assertEquals(3,p.policyVersion());assertEquals("A",p.mappings().get("name").get(0).from());
  pairs.clear();maps.clear();assertEquals(2,p.mappings().get("name").size());
  assertThrows(UnsupportedOperationException.class,()->p.mappings().get("name").clear());
  assertThrows(UnsupportedOperationException.class,()->p.mappings().clear());
 }
 @Test void rejectsDuplicatesUnknownColumnsNullsAndReservedValues(){
  assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(Map.of("unknown",List.of(new Tasks.ValueMapping("a","b"))),3),columns));
  for(var pair:List.of(new Tasks.ValueMapping(null,"x"),new Tasks.ValueMapping("a",null),new Tasks.ValueMapping("\\N","x"),new Tasks.ValueMapping("a","\\N"),new Tasks.ValueMapping("a","\u0000"),new Tasks.ValueMapping("a","\uD800"),new Tasks.ValueMapping("a","x".repeat(1001)))){
   assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(Map.of("name",List.of(pair)),3),columns));
  }
  assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(Map.of("name",List.of(new Tasks.ValueMapping("a","b"),new Tasks.ValueMapping("a","c"))),3),columns));
  assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(Map.of("name",List.of()),3),columns));
 }
 @Test void enforcesPerFieldTotalAndCharacterBudgets(){
  var pairs=new ArrayList<Tasks.ValueMapping>();
  for(int i=0;i<51;i++)pairs.add(new Tasks.ValueMapping(""+i,"x"));
  assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(Map.of("name",pairs),3),columns));
  var total=new HashMap<String,List<Tasks.ValueMapping>>();
  for(String c:columns)total.put(c,new ArrayList<>(pairs.subList(0,41)));
  assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(total,3),columns));
  var large=new ArrayList<Tasks.ValueMapping>();
  for(int i=0;i<21;i++)large.add(new Tasks.ValueMapping(i+"a".repeat(998),"b".repeat(1000)));
  assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(Map.of("name",large),3),columns));
 }
 @Test void preservesBlankUnicodeAndLiteralMappingValues(){
  var pairs=List.of(new Tasks.ValueMapping(""," "),new Tasks.ValueMapping("  A  ","中文,\"x\"\nC:\\files"),new Tasks.ValueMapping("A","A"));
  var p=GovernancePolicies.normalize(request(Map.of("name",pairs),3),columns);
  assertEquals(3,p.mappings().get("name").size());
 }
 @Test void versionTwoCannotSilentlyAcquireMappingSemantics()throws Exception {
  assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(Map.of("name",List.of(new Tasks.ValueMapping("a","b"))),2),columns));
  var p=GovernancePolicies.normalize(new Tasks.GovernanceRequest(true,false),columns);
  assertEquals(2,p.policyVersion());
  assertFalse(new ObjectMapper().writeValueAsString(p).contains("mappings"));
 }
 @Test void rejectsJsonTypeCoercionAndUnknownPairFields(){
  var json=new ObjectMapper();
  for(String value:List.of("{\"from\":1,\"to\":\"x\"}","{\"from\":\"a\",\"to\":true}","{\"from\":null,\"to\":\"x\"}","{\"from\":\"a\",\"to\":\"b\",\"extra\":1}")){
   assertThrows(Exception.class,()->json.readValue(value,Tasks.ValueMapping.class));
  }
 }
}
