package cn.sparkdq.engine;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class DriverNetworkTest {
 @Test void defaultKeepsLoopback(){var v=DriverNetwork.settings(Map.of());assertEquals("local[2]",v.master());assertEquals("127.0.0.1",v.host());assertEquals("127.0.0.1",v.bind());}
 @Test void explicitStandaloneUsesReachableHost(){var v=DriverNetwork.settings(Map.of("SPARK_MASTER","spark://master:7077","SPARK_DQ_DRIVER_HOST","driver"));assertEquals("driver",v.host());assertEquals("0.0.0.0",v.bind());}
 @Test void localHostOverrideRejected(){assertThrows(IllegalArgumentException.class,()->DriverNetwork.settings(Map.of("SPARK_DQ_DRIVER_HOST","driver")));}
 @Test void remoteRequiresHost(){assertThrows(IllegalArgumentException.class,()->DriverNetwork.settings(Map.of("SPARK_MASTER","spark://master:7077")));}
 @Test void invalidOrLoopbackHostsRejected(){for(String host:List.of("localhost","127.0.0.1","0.0.0.0","a..b","a/b","a:7077","-driver","a.-b"))assertThrows(IllegalArgumentException.class,()->DriverNetwork.settings(Map.of("SPARK_MASTER","spark://master:7077","SPARK_DQ_DRIVER_HOST",host)),host);}
 @Test void invalidMasterRejected(){for(String master:List.of("spark://master","http://master:7077","spark://user@master:7077","spark://master:7077/path","spark://master:7077?x=1","local-cluster[2,1,512]"))assertThrows(IllegalArgumentException.class,()->DriverNetwork.settings(Map.of("SPARK_MASTER",master,"SPARK_DQ_DRIVER_HOST","driver")),master);}
}
