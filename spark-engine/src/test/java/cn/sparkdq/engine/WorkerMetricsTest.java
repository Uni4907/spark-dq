package cn.sparkdq.engine;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class WorkerMetricsTest {
 @Test void defaultsPreserveExistingInputAndShuffle(){
  var settings=WorkerMetrics.settings(Map.of());assertEquals(0,settings.inputPartitions());assertEquals(2,settings.shufflePartitions());
 }
 @Test void supportsBoundedOverrides(){
  var settings=WorkerMetrics.settings(Map.of("SPARK_INPUT_PARTITIONS","4","SPARK_SHUFFLE_PARTITIONS","32"));
  assertEquals(4,settings.inputPartitions());assertEquals(32,settings.shufflePartitions());
 }
 @Test void rejectsMalformedAndOutOfRangeOverrides(){
  for(String value:List.of("","0","33","-1","2.0"," 2","1;exit","999999999999")){
   for(String name:List.of("SPARK_INPUT_PARTITIONS","SPARK_SHUFFLE_PARTITIONS"))
    assertThrows(IllegalArgumentException.class,()->WorkerMetrics.settings(Map.of(name,value)));
  }
 }
 @Test void distinguishesUnavailableFromMeasuredRss(){
  assertEquals(12636160L,WorkerMetrics.peakRss("Name:\tjava\nVmHWM:\t12340 kB\nVmRSS:\t10000 kB\n"));
  assertNull(WorkerMetrics.peakRss("VmRSS: 100 kB\n"));
  assertNull(WorkerMetrics.peakRss("VmHWM: 9999999999999999 kB\n"));
 }
}
