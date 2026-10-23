package cn.sparkdq;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class CsvPartsTest {
 @TempDir Path root;
 MockMultipartFile part(String name,String body){return new MockMultipartFile("files",name,"text/csv",body.getBytes(StandardCharsets.UTF_8));}
 CsvParts.Merged merge(MockMultipartFile... files)throws Exception{return CsvParts.merge(List.of(files),root.resolve(UUID.randomUUID().toString()));}
 @Test void preservesOrderDuplicatesSpecialTextAndOriginalBytes()throws Exception{
  var first=part("a.csv","\ufeffid,note\r\nA,\"中文,\n\"\"quote\"\"\"\r\nA,\\N\r\n");
  var second=part("b.csv","id,note\nA,\"\"\nB,   \n");var merged=merge(first,second);
  assertEquals(4,merged.rows());assertEquals(List.of("id","note"),merged.columns());
  assertArrayEquals(first.getBytes(),Files.readAllBytes(merged.csv().getParent().resolve("001.csv")));
  assertEquals(ExecutionInputs.sha(merged.csv().getParent().resolve("002.csv")),merged.files().get(1).get("sha256"));
  var parsed=org.apache.commons.csv.CSVFormat.RFC4180.parse(Files.newBufferedReader(merged.csv())).getRecords();
  assertEquals("中文,\n\"quote\"",parsed.get(1).get(1));assertEquals("\\N",parsed.get(2).get(1));assertEquals("",parsed.get(3).get(1));assertEquals("   ",parsed.get(4).get(1));
 }
 @Test void rejectsBadCountsNamesAndEmptyFiles()throws Exception{
  assertThrows(Exception.class,()->merge(part("one.csv","id\n1\n")));
  assertThrows(Exception.class,()->merge(part("same.csv","id\n1\n"),part("same.csv","id\n2\n")));
  assertThrows(Exception.class,()->merge(part("bad.txt","id\n1\n"),part("good.csv","id\n2\n")));
  assertThrows(Exception.class,()->merge(part("a.csv",""),part("b.csv","id\n2\n")));
  assertThrows(Exception.class,()->CsvParts.merge(Collections.nCopies(17,part("a.csv","id\n1\n")),root.resolve("many")));
 }
 @Test void rejectsSchemaMismatchMalformedUtf8AndOversizedCells()throws Exception{
  for(String content:List.of("name,id\nx,1\n","id,id\n1,2\n","id,note\n1\n","id,note\n1,a\u0000\n","id,note\n1,"+"x".repeat(65537)+"\n","id,note\n1,\"unclosed\n"))
   assertThrows(Exception.class,()->merge(part("a.csv","id,note\n1,x\n"),part("b.csv",content)));
  assertThrows(Exception.class,()->merge(part("a.csv","id\n1\n"),new MockMultipartFile("files","b.csv","text/csv",new byte[]{(byte)0xc3,0x28})));
 }
 @Test void acceptsHeaderOnlyPartButRejectsEmptyCombined()throws Exception{
  assertEquals(1,merge(part("a.csv","id\n"),part("b.csv","id\n1\n")).rows());
  assertThrows(Exception.class,()->merge(part("a.csv","id\n"),part("b.csv","id\n")));
 }
 @Test void enforcesActualAggregateBytesAndCanonicalExpansion()throws Exception{
  String row="x".repeat(60000)+"\n";String large="id\n"+row.repeat(180);
  assertThrows(Exception.class,()->merge(part("a.csv",large),part("b.csv",large)));
  // Small unquoted empty fields expand to quote-all canonical CSV beyond its own limit.
  String header=String.join(",",java.util.stream.IntStream.range(0,100).mapToObj(i->"c"+i).toList())+"\n";
  String dense=header+("1,".repeat(99)+"1\n").repeat(30000);
  assertThrows(Exception.class,()->merge(part("a.csv",dense),part("b.csv",dense)));
 }
 @Test void shardParserUsesTheSameEarlyColumnAllocationGuard()throws Exception{
  var error=assertThrows(org.springframework.web.server.ResponseStatusException.class,()->merge(part("a.csv",",".repeat(100)+"\n"),part("b.csv","id\n1\n")));
  assertEquals("每条 CSV 记录最多100列",error.getReason());
 }
}
