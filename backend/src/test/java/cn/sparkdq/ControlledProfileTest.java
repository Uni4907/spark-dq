package cn.sparkdq;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.junit.jupiter.api.Assertions.*;
class ControlledProfileTest {
 @TempDir Path root;
 @Test void onlyExplicitStoredProfileEnablesLargerVersion(){assertFalse(ControlledProfile.controlled(Map.of("size_bytes",30_000_000)));assertFalse(ControlledProfile.controlled(Map.of("source",Map.of("format","CONTROLLED_CSV","profile","anything"))));assertTrue(ControlledProfile.controlled(Map.of("source",Map.of("format","CONTROLLED_CSV","profile","CONTROLLED_64M_V1"))));}
 @Test void outputBudgetCountsNestedFilesAndRejectsLinks()throws Exception{Files.createDirectories(root.resolve("nested"));Files.write(root.resolve("nested/a"),new byte[9]);Files.write(root.resolve("b"),new byte[7]);assertEquals(16,ControlledProfile.treeBytes(root,16,5));assertThrows(IllegalStateException.class,()->ControlledProfile.treeBytes(root,15,5));Files.createSymbolicLink(root.resolve("link"),root.resolve("b"));assertThrows(IllegalStateException.class,()->ControlledProfile.treeBytes(root,100,10));}
 @Test void directoryEntryBudgetStopsDeepOrFragmentedOutput()throws Exception{Files.createDirectories(root.resolve("a/b/c"));assertThrows(IllegalStateException.class,()->ControlledProfile.treeBytes(root,100,3));}
 @Test void registrationDeadlineIsEnforced(){assertThrows(org.springframework.web.server.ResponseStatusException.class,()->ControlledProfile.checkpoint(System.nanoTime()-1));}
 @Test void whitelistDisabledByDefault(){assertEquals(Map.of(),ControlledFiles.load(new ObjectMapper(),null));}
 @Test void configRejectsArbitraryPathsAndUnknownFields()throws Exception{
  Path config=root.resolve("sources.json");String prefix="{\"files\":[{\"id\":\"demo\",\"name\":\"source.csv\",\"path\":\"/etc/passwd\",\"sha256\":\""+"a".repeat(64)+"\"";
  Files.writeString(config,prefix+"}]}");assertThrows(IllegalStateException.class,()->ControlledFiles.load(new ObjectMapper(),config.toString()));Files.writeString(config,prefix+",\"jdbcUrl\":\"not-accepted\"}]}");assertThrows(IllegalStateException.class,()->ControlledFiles.load(new ObjectMapper(),config.toString()));
 }
}
