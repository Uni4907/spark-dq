package cn.sparkdq;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.util.*;

@Service
public class RuleTemplates {
 final Datasets ds;final TransactionTemplate tx;
 public RuleTemplates(Datasets ds,PlatformTransactionManager manager){this.ds=ds;this.tx=new TransactionTemplate(manager);}
 public record Save(String name,List<Rules.Spec> rules,Integer expectedRevision){}
 String name(Save body){if(body==null||body.name()==null||body.name().isBlank()||body.name().length()>120)throw Datasets.bad("模板名称须为 1～120 字符");return body.name().trim();}
 public List<Map<String,Object>> list(){return ds.db.queryForList("select id,name,revision,archived,updated_at from rule_template_heads where archived=false order by updated_at desc limit 200");}
 static Map<String,Object> head(Datasets ds,String id){
  var rows=ds.db.queryForList("select * from rule_template_heads where id=?",id);
  if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"规则模板不存在");return rows.get(0);
 }
 public Map<String,Object> one(String id,Integer revision){return version(ds,id,revision);}
 static Map<String,Object> version(Datasets ds,String id,Integer revision){
  var h=head(ds,id);int rev=revision==null?((Number)h.get("revision")).intValue():revision;
  var rows=ds.db.queryForList("select template_id,revision,name,definitions_json,created_at from rule_template_versions where template_id=? and revision=?",id,rev);
  if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"模板修订不存在");
  var row=rows.get(0);row.put("rules",ds.decode(row.remove("definitions_json")));row.put("archived",h.get("archived"));return row;
 }
 public List<Map<String,Object>> history(String id){head(ds,id);return ds.db.queryForList("select revision,name,created_at from rule_template_versions where template_id=? order by revision desc",id);}
 public Map<String,Object> create(Save body){
  String title=name(body),id=Datasets.id();var definitions=Rules.normalize(body.rules(),null);
  tx.executeWithoutResult(s->{
   ds.db.update("insert into rule_template_heads(id,name,revision) values(?,?,1)",id,title);
   ds.db.update("insert into rule_template_versions(template_id,revision,name,definitions_json) values(?,1,?,?)",id,title,ds.encode(definitions));
  });return one(id,null);
 }
 public Map<String,Object> update(String id,Save body){
  String title=name(body);if(body.expectedRevision()==null||body.expectedRevision()<1)throw Datasets.bad("更新模板需要 expectedRevision");
  var definitions=Rules.normalize(body.rules(),null);head(ds,id);
  tx.executeWithoutResult(s->{
   int changed=ds.db.update("update rule_template_heads set name=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP(3) where id=? and revision=? and archived=false",title,id,body.expectedRevision());
   if(changed!=1)throw new ResponseStatusException(HttpStatus.CONFLICT,"模板已更新或归档，请刷新后重试");
   ds.db.update("insert into rule_template_versions(template_id,revision,name,definitions_json) values(?,?,?,?)",id,body.expectedRevision()+1,title,ds.encode(definitions));
  });return one(id,body.expectedRevision()+1);
 }
 public Map<String,Object> archive(String id,int revision){
  head(ds,id);
  if(ds.db.update("update rule_template_heads set archived=true,updated_at=CURRENT_TIMESTAMP(3) where id=? and revision=? and archived=false",id,revision)!=1)throw new ResponseStatusException(HttpStatus.CONFLICT,"模板已更新或归档，请刷新后重试");
  return one(id,revision);
 }
 static Map<String,Object> select(Datasets ds,String id,Integer revision){
  var value=version(ds,id,revision);
  if(Boolean.TRUE.equals(value.get("archived"))||Integer.valueOf(1).equals(value.get("archived")))throw new ResponseStatusException(HttpStatus.CONFLICT,"模板已归档，不能用于新任务");
  return value;
 }
}
