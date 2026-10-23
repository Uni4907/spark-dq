package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class RetentionOrderTest {
 List<String> columns=List.of("key","score","day");
 Tasks.SortSpec spec(String column,String type,String direction,String nulls){return new Tasks.SortSpec(column,type,direction,nulls);}
 Tasks.GovernanceRequest request(List<Tasks.SortSpec> order,boolean dedup,Integer version){return new Tasks.GovernanceRequest(false,dedup,Map.of(),Map.of(),Map.of(),order,version);}
 @Test void freezesOrderWithoutSortingAwayPriority(){
  var list=new ArrayList<>(List.of(spec("day","DATE","DESC","LAST"),spec("score","DECIMAL","DESC","FIRST")));
  var p=GovernancePolicies.normalize(request(list,true,null),columns);
  list.clear();assertEquals(4,p.policyVersion());assertEquals("day",p.deduplicationOrder().get(0).column());
  assertThrows(UnsupportedOperationException.class,()->p.deduplicationOrder().clear());
 }
 @Test void requiresDeduplicationAndVersionFour(){
  var order=List.of(spec("score","TEXT","ASC","LAST"));
  assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(order,false,4),columns));
  for(int version:List.of(1,2,3,5))assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(order,true,version),columns));
 }
 @Test void rejectsMissingDuplicateAndTooManyFields(){
  var good=spec("score","DECIMAL","ASC","LAST");
  for(var list:List.of(List.of(spec("unknown","TEXT","ASC","LAST")),List.of(good,good),
    Collections.nCopies(6,good),Arrays.asList((Tasks.SortSpec)null))){
   assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(list,true,4),columns));
  }
 }
 @Test void rejectsNullOrUnknownEnums(){
  for(var item:List.of(spec("score",null,"ASC","LAST"),spec("score","INTEGER","ASC","LAST"),
    spec("score","TEXT",null,"LAST"),spec("score","TEXT","asc","LAST"),
    spec("score","TEXT","ASC",null),spec("score","TEXT","ASC","MIDDLE"))){
   assertThrows(RuntimeException.class,()->GovernancePolicies.normalize(request(List.of(item),true,4),columns));
  }
 }
 @Test void supportsEveryDirectionTypeAndNullPlacement(){
  for(String type:List.of("TEXT","DECIMAL","DATE"))for(String direction:List.of("ASC","DESC"))for(String nulls:List.of("FIRST","LAST")){
   assertEquals(4,GovernancePolicies.normalize(request(List.of(spec("score",type,direction,nulls)),true,4),columns).policyVersion());
  }
 }
 @Test void preservesHistoricalSnapshotShapes()throws Exception{
  var json=new ObjectMapper();
  var v2=GovernancePolicies.normalize(new Tasks.GovernanceRequest(false,true),columns);
  var v3=GovernancePolicies.normalize(new Tasks.GovernanceRequest(false,true,Map.of(),Map.of(),Map.of("score",List.of(new Tasks.ValueMapping("a","b"))),3),columns);
  assertEquals(2,v2.policyVersion());assertEquals(3,v3.policyVersion());
  assertFalse(json.writeValueAsString(v2).contains("deduplicationOrder"));assertFalse(json.writeValueAsString(v3).contains("deduplicationOrder"));
 }
 @Test void rejectsJsonCoercionMissingAndUnknownFields(){
  var json=new ObjectMapper();
  for(String value:List.of("{\"column\":1,\"type\":\"TEXT\",\"direction\":\"ASC\",\"nulls\":\"LAST\"}",
    "{\"column\":\"score\",\"type\":\"TEXT\",\"direction\":\"ASC\"}",
    "{\"column\":\"score\",\"type\":\"TEXT\",\"direction\":\"ASC\",\"nulls\":\"LAST\",\"extra\":1}")){
   assertThrows(Exception.class,()->json.readValue(value,Tasks.SortSpec.class));
  }
 }
}
