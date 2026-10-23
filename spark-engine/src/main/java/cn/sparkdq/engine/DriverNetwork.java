package cn.sparkdq.engine;
import java.net.URI;
import java.util.*;

/** Trusted process configuration only; never accepts network settings in task JSON. */
final class DriverNetwork {
 record Settings(String master,String host,String bind){}
 static Settings settings(Map<String,String> env){
  String master=env.getOrDefault("SPARK_MASTER","local[2]");
  if(master.matches("local(?:\\[(?:[1-9][0-9]*|\\*)(?:,[1-9][0-9]*)?\\])?")){
   if(env.containsKey("SPARK_DQ_DRIVER_HOST"))throw new IllegalArgumentException("Driver host override is only valid for standalone execution");
   return new Settings(master,"127.0.0.1","127.0.0.1");
  }
  URI uri;try{uri=URI.create(master);}catch(IllegalArgumentException e){throw new IllegalArgumentException("Invalid standalone master");}
  if(!"spark".equals(uri.getScheme())||uri.getHost()==null||uri.getPort()<1||uri.getPort()>65535||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null||!uri.getPath().isEmpty())throw new IllegalArgumentException("Only an explicit standalone spark://host:port is supported");
  String host=env.get("SPARK_DQ_DRIVER_HOST");
  if(host==null||host.length()>253||!host.matches("[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?")||Arrays.stream(host.split("\\.",-1)).anyMatch(label->label.isEmpty()||label.length()>63||label.startsWith("-")||label.endsWith("-"))||host.equalsIgnoreCase("localhost")||host.equals("0.0.0.0")||host.startsWith("127."))throw new IllegalArgumentException("Standalone execution requires an explicit reachable SPARK_DQ_DRIVER_HOST");
  return new Settings(master,host,"0.0.0.0");
 }
}
