package cn.sparkdq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class MysqlSourcesTest {
 @TempDir Path root;final ObjectMapper json=new ObjectMapper();
 Map<String,Object> entry(){return new LinkedHashMap<>(Map.of("id","business","host","business-mysql","user","read_only","passwordFile",root.resolve("secret").toString(),"schema","business","table","sample","columns",List.of(Map.of("source","金额","target","amount"))));}
 Map<String,MysqlSources.Source> load(Map<String,Object> entry)throws Exception {Path config=root.resolve(UUID.randomUUID()+".json");json.writeValue(config.toFile(),Map.of("sources",List.of(entry)));return MysqlSources.load(json,config.toString());}
 @Test void disabledByDefault(){assertTrue(MysqlSources.load(json,null).isEmpty());}
 @Test void listsOnlyNonSecretMetadata()throws Exception {var sources=new MysqlSources(load(entry()));var view=json.writeValueAsString(sources.list());assertTrue(view.contains("amount"));for(String secret:List.of("read_only","business-mysql","passwordFile",root.toString()))assertFalse(view.contains(secret));}
 @Test void rejectsUrlInjectionAndUnsupportedTls()throws Exception {
  for(var pair:Map.of("host","host/?user=root","tlsMode","PREFERRED","schema","x/metadata","port",0).entrySet()){
   var entry=entry();entry.put(pair.getKey(),pair.getValue());var e=assertThrows(IllegalStateException.class,()->load(entry));assertEquals("MYSQL_SOURCES_CONFIGURATION_INVALID",e.getMessage());assertNull(e.getCause());
  }
 }
 @Test void rejectsUnknownFieldsAndExcessiveBounds()throws Exception {for(var pair:Map.of("password","should-not-be-inline","maxBytes",MysqlSnapshot.MAX_BYTES+1,"timeoutSeconds",301).entrySet()){var entry=entry();entry.put(pair.getKey(),pair.getValue());assertThrows(IllegalStateException.class,()->load(entry));}}
 @Test void secretsCannotBeBackedUpAsDataFiles()throws Exception {var entry=entry();entry.put("passwordFile","/data/source-secret");assertThrows(IllegalStateException.class,()->load(entry));assertThrows(IllegalStateException.class,()->MysqlSources.load(json,"/data/sources.json"));}
 @Test void duplicateSourceIdsFailClosed()throws Exception {var entry=entry();Path file=root.resolve("duplicate.json");json.writeValue(file.toFile(),Map.of("sources",List.of(entry,entry)));assertThrows(IllegalStateException.class,()->MysqlSources.load(json,file.toString()));}
 @Test void connectionErrorsHaveNoHostOrCredentialCause()throws Exception {var source=load(entry()).get("business");var error=assertThrows(java.sql.SQLException.class,source::open);assertEquals("MYSQL_SOURCE_CONNECTION_FAILED",error.getMessage());assertNull(error.getCause());}
 @Test void secretSymlinkIsRejected()throws Exception {Files.writeString(root.resolve("real"),"synthetic-test-password");Files.createSymbolicLink(root.resolve("secret"),root.resolve("real"));assertThrows(java.sql.SQLException.class,load(entry()).get("business")::open);}
}
