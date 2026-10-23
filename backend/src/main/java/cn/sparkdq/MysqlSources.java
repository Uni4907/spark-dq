package cn.sparkdq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Private server-owned configuration. No request supplies a JDBC URL or credentials. */
@Service
public class MysqlSources {
 final Map<String,Source> sources;
 static final class Source {
  final String id,host,user,tls;final int port;final Path password;final MysqlSnapshot.Table table;final MysqlSnapshot.Limits limits;
  Source(String id,String host,int port,String user,String tls,Path password,MysqlSnapshot.Table table,MysqlSnapshot.Limits limits){
   if(id==null||!id.matches("[A-Za-z][A-Za-z0-9_-]{0,63}")||host==null||!host.matches("[A-Za-z0-9][A-Za-z0-9.-]{0,252}")||port<1||port>65535||user==null||user.isBlank()||user.length()>64||!Set.of("DISABLED","VERIFY_IDENTITY").contains(tls)||!password.isAbsolute()||password.normalize().startsWith(Datasets.ROOT))throw new IllegalArgumentException();
   this.id=id;this.host=host;this.port=port;this.user=user;this.tls=tls;this.password=password;this.table=table;this.limits=limits;
  }
  Connection open()throws SQLException {
   try {
    if(!Files.isRegularFile(password,LinkOption.NOFOLLOW_LINKS)||Files.size(password)>4096)throw new IllegalArgumentException();
    var properties=new Properties();properties.setProperty("user",user);properties.setProperty("password",Files.readString(password).stripTrailing());
    properties.setProperty("connectTimeout","3000");properties.setProperty("socketTimeout",Integer.toString((limits.seconds()+2)*1000));
    properties.setProperty("sslMode",tls);properties.setProperty("allowPublicKeyRetrieval",Boolean.toString(tls.equals("DISABLED")));
    properties.setProperty("zeroDateTimeBehavior","EXCEPTION");properties.setProperty("tinyInt1isBit","false");properties.setProperty("connectionTimeZone","UTC");
    return DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+table.schema(),properties);
   }catch(Exception error){throw new SQLException("MYSQL_SOURCE_CONNECTION_FAILED");}
  }
  Map<String,Object> view(){return Map.of("id",id,"schema",table.schema(),"table",table.name(),"columns",table.allowed(),"limits",Map.of("rows",limits.rows(),"bytes",limits.bytes(),"seconds",limits.seconds()),"mode","MySQL业务快照导入");}
 }
 @org.springframework.beans.factory.annotation.Autowired public MysqlSources(ObjectMapper json){this(load(json,System.getenv("SPARK_DQ_MYSQL_SOURCES_FILE")));}
 MysqlSources(Map<String,Source> sources){this.sources=Map.copyOf(sources);}
 static Map<String,Source> load(ObjectMapper json,String filename){
  if(filename==null||filename.isBlank())return Map.of();
  try {
   Path file=Path.of(filename);if(!file.isAbsolute()||file.normalize().startsWith(Datasets.ROOT)||!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)||Files.size(file)>65536)throw new IllegalArgumentException();
   var root=json.readTree(file.toFile());if(!root.isObject()||root.size()!=1||!root.path("sources").isArray()||root.path("sources").size()>16)throw new IllegalArgumentException();
   var result=new LinkedHashMap<String,Source>();
   for(var entry:root.path("sources")){
    var allowed=Set.of("id","host","port","user","tlsMode","passwordFile","schema","table","columns","maxRows","maxBytes","timeoutSeconds");
    var keys=entry.fieldNames();while(keys.hasNext())if(!allowed.contains(keys.next()))throw new IllegalArgumentException();
    var columns=new ArrayList<MysqlSnapshot.Column>();if(!entry.path("columns").isArray())throw new IllegalArgumentException();
    for(var column:entry.path("columns")){if(column.size()!=2)throw new IllegalArgumentException();columns.add(new MysqlSnapshot.Column(column.path("source").textValue(),column.path("target").textValue()));}
    var table=new MysqlSnapshot.Table(entry.path("schema").textValue(),entry.path("table").textValue(),columns);
    var limits=new MysqlSnapshot.Limits(entry.path("maxRows").asLong(1_000_000),entry.path("maxBytes").asLong(MysqlSnapshot.MAX_BYTES),entry.path("timeoutSeconds").asInt(30));
    var source=new Source(entry.path("id").textValue(),entry.path("host").textValue(),entry.path("port").asInt(3306),entry.path("user").textValue(),entry.path("tlsMode").asText("VERIFY_IDENTITY"),Path.of(entry.path("passwordFile").asText()),table,limits);
    if(result.putIfAbsent(source.id,source)!=null)throw new IllegalArgumentException();
   }
   return result;
  }catch(Exception error){throw new IllegalStateException("MYSQL_SOURCES_CONFIGURATION_INVALID");}
 }
 Source source(String id){var source=sources.get(id);if(source==null)throw Datasets.bad("MySQL业务源未配置或不在白名单");return source;}
 public List<Map<String,Object>> list(){return sources.values().stream().sorted(Comparator.comparing(s->s.id)).map(Source::view).toList();}
}
