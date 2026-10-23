package cn.sparkdq;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class DatabaseMigrationsTest {
 @Test void completeLegacySchemaAccepted() {
  assertDoesNotThrow(()->DatabaseMigrations.requireLegacy(DatabaseMigrations.resource("db/legacy-metadata.txt").lines().toList()));
 }
 @Test void incompleteLegacySchemaRefused() {
  var metadata=new ArrayList<>(DatabaseMigrations.resource("db/legacy-metadata.txt").lines().toList());
  metadata.remove(0);
  assertThrows(IllegalStateException.class,()->DatabaseMigrations.requireLegacy(metadata));
 }
 @Test void modifiedAndAdditionalObjectsRefused() {
  var metadata=new ArrayList<>(DatabaseMigrations.resource("db/legacy-metadata.txt").lines().toList());
  metadata.set(1,metadata.get(1).replace("char(36)","char(35)"));
  assertThrows(IllegalStateException.class,()->DatabaseMigrations.requireLegacy(metadata));
  var additional=new ArrayList<>(DatabaseMigrations.resource("db/legacy-metadata.txt").lines().toList());
  additional.add("[\"trigger\", \"unexpected_trigger\"]");
  assertThrows(IllegalStateException.class,()->DatabaseMigrations.requireLegacy(additional));
 }
}
