package cn.sparkdq.engine;
import java.lang.management.ManagementFactory;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

/** Optional local execution settings and measured worker-process resources; no dataset contents. */
public final class WorkerMetrics {
 public record Settings(int inputPartitions,int shufflePartitions){}
 public static Settings settings(Map<String,String> env){
  return new Settings(partitions(env,"SPARK_INPUT_PARTITIONS",0),partitions(env,"SPARK_SHUFFLE_PARTITIONS",2));
 }
 static int partitions(Map<String,String> env,String name,int fallback){
  if(!env.containsKey(name))return fallback;
  String value=env.get(name);
  if(value==null||!value.matches("[0-9]{1,2}"))throw new IllegalArgumentException(name+" must be an integer in 1..32");
  int count=Integer.parseInt(value);
  if(count<1||count>32)throw new IllegalArgumentException(name+" must be an integer in 1..32");
  return count;
 }
 public static long millisSince(long started){return (System.nanoTime()-started)/1_000_000;}
 static Long peakRss(String status){
  var matcher=Pattern.compile("(?m)^VmHWM:\\s*([0-9]+)\\s+kB\\s*$").matcher(status);
  if(!matcher.find())return null;
  try{return Math.multiplyExact(Long.parseLong(matcher.group(1)),1024L);}catch(ArithmeticException error){return null;}
 }
 public static Map<String,Object> resources(){
  var result=new LinkedHashMap<String,Object>();
  result.put("maxHeapBytes",Runtime.getRuntime().maxMemory());
  var bean=ManagementFactory.getOperatingSystemMXBean();
  if(bean instanceof com.sun.management.OperatingSystemMXBean os){
   long cpu=os.getProcessCpuTime();if(cpu>=0)result.put("workerCpuMs",cpu/1_000_000);
  }
  try{
   Long peak=peakRss(Files.readString(Path.of("/proc/self/status")));
   if(peak!=null)result.put("peakRssBytes",peak);
  }catch(Exception unavailable){/* Omit unsupported observations rather than invent zero. */}
  return result;
 }
}
