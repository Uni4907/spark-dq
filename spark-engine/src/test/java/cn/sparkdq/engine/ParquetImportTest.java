package cn.sparkdq.engine;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.io.*;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;
class ParquetImportTest {
 @TempDir Path root;
 @Test void scalarContractRejectsApproximateAndAmbiguousTypes(){
  for(DataType t:List.of(DataTypes.FloatType,DataTypes.DoubleType,DataTypes.TimestampType,DataTypes.TimestampNTZType,DataTypes.BinaryType,DataTypes.createArrayType(DataTypes.StringType),DataTypes.createMapType(DataTypes.StringType,DataTypes.StringType),DataTypes.createStructType(new StructField[0])))
   assertThrows(Exception.class,()->ParquetImport.schema(DataTypes.createStructType(new StructField[]{DataTypes.createStructField("id",t,true)})));
  for(DataType t:List.of(DataTypes.StringType,DataTypes.BooleanType,DataTypes.ByteType,DataTypes.ShortType,DataTypes.IntegerType,DataTypes.LongType,DataTypes.DateType,DataTypes.createDecimalType(38,10)))
   assertEquals(t.catalogString(),ParquetImport.schema(DataTypes.createStructType(new StructField[]{DataTypes.createStructField("id",t,true)})).get(0).get("type"));
 }
 @Test void rejectsEmptyDuplicateUnsafeAndTooManyFields(){
  assertThrows(Exception.class,()->ParquetImport.schema(DataTypes.createStructType(new StructField[0])));
  var same=DataTypes.createStructField("id",DataTypes.StringType,true);
  assertThrows(Exception.class,()->ParquetImport.schema(DataTypes.createStructType(new StructField[]{same,same})));
  assertThrows(Exception.class,()->ParquetImport.schema(DataTypes.createStructType(new StructField[]{DataTypes.createStructField("a.b",DataTypes.StringType,true)})));
  assertThrows(Exception.class,()->ParquetImport.schema(DataTypes.createStructType(java.util.stream.IntStream.range(0,101).mapToObj(i->DataTypes.createStructField("c"+i,DataTypes.StringType,true)).toArray(StructField[]::new))));
 }
 @Test void strictStringsPreserveBlankAndUnicodeButRejectLoss()throws Exception{
  for(String text:List.of("","   ","中文🙂\n\"quote\",","literal \\N is fine"))assertEquals(text,ParquetImport.text(text.getBytes(StandardCharsets.UTF_8),DataTypes.StringType));
  assertNull(ParquetImport.text(null,DataTypes.StringType));
  for(String text:List.of("\\N","nul\u0000","x".repeat(65537),"CR\rLF\n","raw\r"))assertThrows(Exception.class,()->ParquetImport.text(text.getBytes(StandardCharsets.UTF_8),DataTypes.StringType));
  assertThrows(Exception.class,()->ParquetImport.text(new byte[]{(byte)0xc3,0x28},DataTypes.StringType));
 }
 @Test void decimalIntegerBooleanAndDateAreExplicit()throws Exception{
  assertEquals("1234567890123456789012345678.1234567890",ParquetImport.text(new BigDecimal("1234567890123456789012345678.1234567890"),DataTypes.createDecimalType(38,10)));
  assertEquals("-9223372036854775808",ParquetImport.text(Long.MIN_VALUE,DataTypes.LongType));assertEquals("true",ParquetImport.text(true,DataTypes.BooleanType));
  assertEquals("2024-02-29",ParquetImport.text("2024-02-29",DataTypes.DateType));
  assertThrows(Exception.class,()->ParquetImport.text("1582-10-14",DataTypes.DateType));
 }
 @Test void csvQuotesNullAndSpecialValuesWithoutConflation()throws Exception{
  var writer=new StringWriter();ParquetImport.record(writer,Arrays.asList(null,""," ","a,\n\"b"));assertEquals("\"\\N\",\"\",\" \",\"a,\n\"\"b\"\r\n",writer.toString());
 }
 @Test void actualByteBudgetIncludesUtf8AndCannotOverflow()throws Exception{
  var out=new ParquetImport.Bounded(OutputStream.nullOutputStream());out.bytes=ParquetImport.MAX_BYTES-3;out.write("中".getBytes(StandardCharsets.UTF_8));assertThrows(Exception.class,()->out.write('x'));assertEquals(ParquetImport.MAX_BYTES,out.bytes);
 }
 @Test void pinsSourceRejectsRequestOverridesExistingOutputAndLinks()throws Exception{
  Path input=root.resolve("source.parquet");Files.write(input,"PAR1xxxxPAR1".getBytes());var node=ParquetImport.JSON.createObjectNode().put("input",input.toString()).put("output",root.resolve("new").toString()).put("sourceSha256",PreparedInput.sha(input));
  assertEquals(input,ParquetImport.request(node).input());node.put("sourceSha256","a".repeat(64));assertThrows(Exception.class,()->ParquetImport.request(node));node.put("sourceSha256",PreparedInput.sha(input));
  node.put("options","arbitrary");assertThrows(Exception.class,()->ParquetImport.request(node));node.remove("options");Files.createDirectory(root.resolve("new"));assertThrows(Exception.class,()->ParquetImport.request(node));
  node.put("output",root.resolve("other").toString());Path link=root.resolve("link.parquet");Files.createSymbolicLink(link,input);node.put("input",link.toString());assertThrows(Exception.class,()->ParquetImport.request(node));
 }
}
