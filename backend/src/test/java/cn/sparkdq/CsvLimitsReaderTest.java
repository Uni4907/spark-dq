package cn.sparkdq;
import java.io.*;
import java.util.*;
import org.apache.commons.csv.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class CsvLimitsReaderTest {
 List<List<String>> parse(String text)throws Exception{
  try(var reader=new CsvLimitsReader(new StringReader(text),Long.MAX_VALUE);var csv=CSVFormat.RFC4180.builder().setIgnoreEmptyLines(true).get().parse(reader)){var result=new ArrayList<List<String>>();for(var row:csv)result.add(row.toList());return result;}
 }
 @Test void maximumQuotedAndEscapedValuesKeepExactCommonsCsvSemantics()throws Exception{
  String quoted="\"".repeat(65536);assertEquals(List.of(List.of(quoted,"", "a\r\nb", "x\"y")),parse("\""+quoted.replace("\"","\"\"")+"\",\"\",\"a\r\nb\",x\"y\r\n"));
 }
 @Test void largeFieldFailsBeforeParserCanAllocateWholeRecord()throws Exception{
  class Counting extends Reader {int count;public int read(char[] b,int o,int n){int size=Math.min(n,200000-count);if(size==0)return -1;Arrays.fill(b,o,o+size,'x');count+=size;return size;}public void close(){}}
  var source=new Counting();assertThrows(org.springframework.web.server.ResponseStatusException.class,()->{try(var reader=new CsvLimitsReader(source,Long.MAX_VALUE);var csv=CSVFormat.RFC4180.parse(reader)){csv.getRecords();}});assertTrue(source.count<=65536+8192,"Must fail while streaming, before reading full oversized field");
 }
 @Test void headerAndEveryDataRecordHaveEarlyColumnBounds()throws Exception{assertThrows(org.springframework.web.server.ResponseStatusException.class,()->parse(",".repeat(100)));assertThrows(org.springframework.web.server.ResponseStatusException.class,()->parse("a\r\n"+",".repeat(100)));assertEquals(100,parse(",".repeat(99)).get(0).size());}
 @Test void quotedDelimitersAndNewlinesDoNotResetBudgets()throws Exception{assertThrows(org.springframework.web.server.ResponseStatusException.class,()->parse("\""+"x,\r\n".repeat(17000)+"\""));assertThrows(org.springframework.web.server.ResponseStatusException.class,()->parse("\"\""+" ".repeat(CsvLimitsReader.RAW_FIELD)));}
 @Test void nulAndExpiredDeadlineAreRejected(){assertThrows(org.springframework.web.server.ResponseStatusException.class,()->parse("a\u0000b"));assertThrows(org.springframework.web.server.ResponseStatusException.class,()->new CsvLimitsReader(new StringReader("x"),System.nanoTime()-1).read());}
 @Test void incrementalReadsAcrossEscapedQuotesPreserveState()throws Exception{String text="\"a\"\"b\",\"c\r\nd\",e\r\n";try(var reader=new CsvLimitsReader(new StringReader(text),Long.MAX_VALUE)){StringBuilder out=new StringBuilder();for(int c;(c=reader.read())!=-1;)out.append((char)c);assertEquals(text,out.toString());}}
 @Test void previewIsBoundedWithoutBreakingSurrogatePairs(){String text="a".repeat(254)+"😀"+"x".repeat(20);String preview=CsvLimitsReader.preview(text);assertTrue(preview.length()<=256);assertFalse(Character.isHighSurrogate(preview.charAt(preview.length()-2)));assertEquals("small",CsvLimitsReader.preview("small"));assertEquals(List.of(List.of(preview)),CsvLimitsReader.previewRows(List.of(List.of(text))));}
 @Test void anomalyPreviewBoundsValuesButPreservesNullAndDuplicateMultiplicity(){var row=new LinkedHashMap<String,Object>();row.put("text","x".repeat(65536));row.put("empty",null);row.put("__dq_group_size",2L);var bounded=(Map<?,?>)CsvLimitsReader.previewObject(row);assertEquals(256,bounded.get("text").toString().length());assertNull(bounded.get("empty"));assertEquals(2L,bounded.get("__dq_group_size"));assertEquals(65536,row.get("text").toString().length());}
}
