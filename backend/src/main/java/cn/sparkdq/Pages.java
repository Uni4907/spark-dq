package cn.sparkdq;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;

/** Bounded, deterministic pages; count and items share one database read snapshot. */
@Service
public class Pages {
 final JdbcTemplate db;
 static final Set<String> STATUSES=Set.of("QUEUED","RUNNING","CANCELLING","CANCELLED","SUCCEEDED","FAILED");
 public Pages(JdbcTemplate db){this.db=db;}
 public record Page(List<Map<String,Object>> items,long total,int page,int pageSize,long totalPages){}
 record Bounds(int page,int size){
  long offset(){return ((long)page-1)*size;}
 }
 static int integer(String raw,int fallback,String label){
  if(raw==null)return fallback;
  try{
   if(!raw.matches("[0-9]+"))throw new NumberFormatException();
   int value=Integer.parseInt(raw);
   if(value<1)throw new NumberFormatException();
   return value;
  }catch(NumberFormatException e){throw Datasets.bad(label+"须为正整数");}
 }
 static Bounds bounds(String page,String size){
  int p=integer(page,1,"page"),s=integer(size,20,"pageSize");
  if(s>100)throw Datasets.bad("pageSize 不能超过 100");
  return new Bounds(p,s);
 }
 Page read(String countSql,String listSql,List<Object> filters,Bounds bounds){
  Long total=db.queryForObject(countSql,Long.class,filters.toArray());
  if(total==null||total<0)throw new IllegalStateException("分页总数无效");
  var args=new ArrayList<>(filters);args.add(bounds.size());args.add(bounds.offset());
  var items=db.queryForList(listSql+" order by t.created_at desc,t.id desc limit ? offset ?",args.toArray());
  return new Page(items,total,bounds.page(),bounds.size(),total/bounds.size()+(total%bounds.size()==0?0:1));
 }
 @Transactional(readOnly=true,isolation=Isolation.REPEATABLE_READ)
 public Page tasks(String page,String size,String status){
  var b=bounds(page,size);
  status=status==null?"ALL":status;
  if(!status.equals("ALL")&&!STATUSES.contains(status))throw Datasets.bad("不支持的任务状态");
  String where=status.equals("ALL")?"":" where t.status=?";
  var args=new ArrayList<Object>();if(!status.equals("ALL"))args.add(status);
  return read("select count(*) from tasks t"+where,
   "select t.*,d.name as dataset_name,g.task_id as governance_id from tasks t join datasets d on t.dataset_id=d.id left join governance_runs g on g.task_id=t.id"+where,args,b);
 }
 @Transactional(readOnly=true,isolation=Isolation.REPEATABLE_READ)
 public Page datasetTasks(String id,String page,String size,String status){
  var b=bounds(page,size);status=status==null?"ALL":status;
  if(!status.equals("ALL")&&!STATUSES.contains(status))throw Datasets.bad("不支持的任务状态");
  if(id==null||id.isBlank()||id.length()>36)throw Datasets.bad("数据集编号无效");
  if(!Long.valueOf(1).equals(db.queryForObject("select count(*) from datasets where id=?",Long.class,id)))throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.NOT_FOUND,"数据集不存在");
  String joins=" from tasks t left join governance_runs g on g.task_id=t.id";
  String where=" where (t.dataset_id=? or g.output_dataset_id=?)"+(status.equals("ALL")?"":" and t.status=?");
  var args=new ArrayList<Object>(List.of(id,id));if(!status.equals("ALL"))args.add(status);
  return read("select count(*)"+joins+where,
   "select t.*,d.name as dataset_name,g.task_id as governance_id,g.output_dataset_id,s.parent_task_id"+joins+" join datasets d on d.id=t.dataset_id left join task_snapshots s on s.task_id=t.id"+where,args,b);
 }
 @Transactional(readOnly=true,isolation=Isolation.REPEATABLE_READ)
 public Page reusablePolicies(String page,String size,String query){
  var b=bounds(page,size);if(query!=null&&query.length()>200)throw Datasets.bad("策略搜索不能超过 200 字符");
  String q=query==null?"":query.strip();String joins=" from tasks t join governance_runs g on g.task_id=t.id join datasets d on d.id=t.dataset_id";
  String where=" where t.status='SUCCEEDED' and g.output_dataset_id is not null"+(q.isEmpty()?"":" and (locate(?,d.name)>0 or locate(?,t.id)>0)");
  var args=new ArrayList<Object>();if(!q.isEmpty()){args.add(q);args.add(q);}
  return read("select count(*)"+joins+where,"select t.id,t.created_at,d.name as dataset_name,g.input_rows,g.output_rows,g.output_dataset_id"+joins+where,args,b);
 }
 @Transactional(readOnly=true,isolation=Isolation.REPEATABLE_READ)
 public Page datasets(String page,String size,String query){
  var b=bounds(page,size);
  if(query!=null&&query.length()>200)throw Datasets.bad("数据集名称搜索不能超过 200 字符");
  String q=query==null?"":query.strip();
  // LOCATE treats %, _ and quotes as literal characters, with prepared parameters.
  String where=q.isEmpty()?"":" where locate(?,t.name)>0";
  var args=new ArrayList<Object>();if(!q.isEmpty())args.add(q);
  return read("select count(*) from datasets t"+where,
   "select t.id,t.name,t.size_bytes,t.row_count,t.created_at from datasets t"+where,args,b);
 }
}
