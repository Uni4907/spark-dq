package cn.sparkdq.engine;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
class ClusterEvidenceTest {
 @TempDir Path root;
 @Test void countsActualTasksAndFailures(){var listener=new ClusterEvidence(root.resolve("proof.json"));listener.task("1","worker1",true);listener.task("2","worker2",true);listener.task("1","worker1",false);var nodes=(List<?>)listener.snapshot().get("executors");assertEquals(2,nodes.size());assertEquals(1L,((Map<?,?>)nodes.get(0)).get("successfulTasks"));assertEquals(1L,((Map<?,?>)nodes.get(0)).get("failedTasks"));}
 @Test void reusedExecutorHostOrBudgetIsNotSilentlyAccepted(){var listener=new ClusterEvidence(root.resolve("proof.json"));listener.task("1","worker1",true);listener.task("1","other",true);assertEquals(true,listener.snapshot().get("truncated"));var bounded=new ClusterEvidence(root.resolve("other.json"));for(int i=0;i<65;i++)bounded.task("e"+i,"worker",true);assertEquals(true,bounded.snapshot().get("truncated"));assertEquals(64,((List<?>)bounded.snapshot().get("executors")).size());}
 @Test void pathMustBeExplicitAbsolute(){assertThrows(IllegalArgumentException.class,()->ClusterEvidence.path(null));assertThrows(IllegalArgumentException.class,()->ClusterEvidence.path("relative"));assertThrows(IllegalArgumentException.class,()->ClusterEvidence.path("/tmp/a/../b"));}
}
