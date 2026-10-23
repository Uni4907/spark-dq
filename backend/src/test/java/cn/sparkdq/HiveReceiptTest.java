package cn.sparkdq;
import com.fasterxml.jackson.databind.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class HiveReceiptTest {
 final ObjectMapper json=new ObjectMapper();
 com.fasterxml.jackson.databind.node.ObjectNode valid(){return json.valueToTree(Map.of("version",1,"catalog","HIVE","database","lab","table","quality","provider","hive","metastore","EMBEDDED_DERBY","sparkVersion","3.5.8","rows",14,"columns",List.of("id","n"),"canonicalSha256","a".repeat(64)));}
 @Test void exactTypedReceiptAccepted(){assertDoesNotThrow(()->HiveReceipt.validate(valid(),"a".repeat(64)));}
 @Test void changedFingerprintAndUnboundedExportRejected(){assertThrows(IllegalArgumentException.class,()->HiveReceipt.validate(valid(),"b".repeat(64)));var v=valid();v.put("rows",101);assertThrows(IllegalArgumentException.class,()->HiveReceipt.validate(v,"a".repeat(64)));}
 @Test void nonHiveCatalogAndArbitrarySqlRejected(){var v=valid();v.put("provider","parquet");assertThrows(IllegalArgumentException.class,()->HiveReceipt.validate(v,"a".repeat(64)));var invalid=valid();invalid.put("table","lab; DROP TABLE x");assertThrows(IllegalArgumentException.class,()->HiveReceipt.validate(invalid,"a".repeat(64)));}
 @Test void extraFieldsAndDuplicateColumnsRejected(){var v=valid();v.put("password","forbidden");assertThrows(IllegalArgumentException.class,()->HiveReceipt.validate(v,"a".repeat(64)));var duplicate=valid();duplicate.set("columns",json.valueToTree(List.of("id","id")));assertThrows(IllegalArgumentException.class,()->HiveReceipt.validate(duplicate,"a".repeat(64)));}
}
