package cn.sparkdq;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
class NativeProofTest {
 @TempDir Path root;ObjectMapper json=new ObjectMapper();ObjectNode manifest;
 @BeforeEach void setup()throws Exception {
  Files.createDirectory(root.resolve("prepared"));Files.writeString(root.resolve("source.parquet"),"PAR1samplebytesPAR1");Files.writeString(root.resolve("prepared/canonical.csv"),"\"a\"\r\n\"1\"\r\n");
  manifest=json.createObjectNode();manifest.put("version",1).put("format","PARQUET_NATIVE").put("semanticsVersion","csv-v2").put("verified",true).put("sourceSha256",ExecutionInputs.sha(root.resolve("source.parquet"))).put("sourceBytes",Files.size(root.resolve("source.parquet"))).put("rowCount",1);
  manifest.set("columns",json.valueToTree(List.of(Map.of("name","a","type","bigint","nullable",true))));manifest.set("conversion",json.valueToTree(NativeProof.CONVERSION));manifest.set("canonical",json.valueToTree(Map.of("path","canonical.csv","size",Files.size(root.resolve("prepared/canonical.csv")),"sha256",ExecutionInputs.sha(root.resolve("prepared/canonical.csv")))));save();
 }
 void save()throws Exception{json.writeValue(root.resolve("prepared/manifest.json").toFile(),manifest);}
 NativeProof.Verified verify()throws Exception{return NativeProof.verify(root,manifest.path("sourceSha256").asText(),manifest.path("sourceBytes").asLong(),json);}
 @Test void bindsSourceManifestAndCanonical()throws Exception{var result=verify();assertEquals(List.of("a"),result.columns());assertEquals(1,result.rows());assertEquals(ExecutionInputs.sha(root.resolve("prepared/manifest.json")),result.manifestSha());}
 @Test void tamperedCanonicalRejected()throws Exception{Files.writeString(root.resolve("prepared/canonical.csv"),"a\n2\n");assertThrows(IllegalStateException.class,this::verify);}
 @Test void tamperedOriginalRejected()throws Exception{Files.writeString(root.resolve("source.parquet"),"PAR1otherbytes!PAR1");assertThrows(IllegalStateException.class,this::verify);}
 @Test void contractAndTypesRejected()throws Exception{manifest.withObject("conversion").put("timezone","LOCAL");save();assertThrows(IllegalStateException.class,this::verify);manifest.set("conversion",json.valueToTree(NativeProof.CONVERSION));((ObjectNode)manifest.path("columns").get(0)).put("type","double");save();assertThrows(IllegalStateException.class,this::verify);}
 @Test void invalidCountsAndDuplicateColumnsRejected()throws Exception{manifest.put("rowCount",0);save();assertThrows(IllegalStateException.class,this::verify);manifest.put("rowCount",1);((ArrayNode)manifest.path("columns")).add(manifest.path("columns").get(0).deepCopy());save();assertThrows(IllegalStateException.class,this::verify);}
 @Test void symlinkRejected()throws Exception{Path canonical=root.resolve("prepared/canonical.csv");Files.move(canonical,root.resolve("else.csv"));Files.createSymbolicLink(canonical,root.resolve("else.csv"));assertThrows(IllegalStateException.class,this::verify);}
 @Test void decimalBoundsRejected()throws Exception{((ObjectNode)manifest.path("columns").get(0)).put("type","decimal(39,0)");save();assertThrows(IllegalStateException.class,this::verify);}
}
