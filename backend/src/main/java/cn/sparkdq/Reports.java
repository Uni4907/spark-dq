package cn.sparkdq;

import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;

@Service
public class Reports {
 final Tasks tasks;
 public Reports(Tasks tasks){this.tasks=tasks;}
 public Map<String,Object> report(String id){
  var task=tasks.detail(id);
  if(!Set.of("SUCCEEDED","FAILED","CANCELLED").contains(task.get("status")))throw new ResponseStatusException(HttpStatus.CONFLICT,"任务结束后才能生成报告");
  var out=new LinkedHashMap<String,Object>();
  out.put("formatVersion","sparkdq-report-v1");out.put("task",task);
  out.put("note","FD异常数为全部冲突参与行，不推断错误来源；跳过原因计数可重叠。各规则异常可能重叠，不合计为总异常行数。IQR每次复检重新拟合阈值，须结合前后Q1/Q3及上下界解释，样本不足不作正常结论；减少异常记录不代表修复原始值；请同时查看输入/输出行数与治理策略。");
  if("SUCCEEDED".equals(task.get("status"))&&task.get("governance") instanceof Map<?,?> run&&run.get("output_dataset_id")!=null)out.put("outputDataset",tasks.ds.one(run.get("output_dataset_id").toString()));
  if(task.get("snapshot") instanceof Map<?,?> snapshot&&snapshot.get("parent_task_id")!=null){
   var parent=tasks.detail(snapshot.get("parent_task_id").toString());
   if("SUCCEEDED".equals(task.get("status"))&&"SUCCEEDED".equals(parent.get("status"))
     &&parent.get("snapshot") instanceof Map<?,?> beforeSnapshot&&Objects.equals(snapshot.get("semantics_version"),beforeSnapshot.get("semantics_version"))){
    out.put("sourceTaskId",parent.get("id"));out.put("beforeRows",parent.get("total_rows"));out.put("afterRows",task.get("total_rows"));
    @SuppressWarnings("unchecked") var before=(List<Map<String,Object>>)parent.get("rules");
    @SuppressWarnings("unchecked") var after=(List<Map<String,Object>>)task.get("rules");
    out.put("comparison",compare(before,after));
   }else out.put("comparisonUnavailable","来源缺少成功结果或兼容的统计口径快照");
  }
  return out;
 }
 static List<Map<String,Object>> compare(List<Map<String,Object>> before,List<Map<String,Object>> after){
  var rows=new ArrayList<Map<String,Object>>();
  for(var rule:after){
   var prior=before.stream().filter(r->Objects.equals(r.get("type"),rule.get("type"))&&Objects.equals(r.get("columns"),rule.get("columns"))&&Objects.equals(r.getOrDefault("params",Map.of()),rule.getOrDefault("params",Map.of()))).findFirst();
   if(prior.isEmpty())continue;
   var oldMetrics=(Map<?,?>)prior.get().get("metrics");var newMetrics=(Map<?,?>)rule.get("metrics");
   String metric="MISSING".equals(rule.get("type"))?"blankCount":"DUPLICATE".equals(rule.get("type"))?"duplicateRows":"invalidCount";
   long oldCount=((Number)oldMetrics.get(metric)).longValue(),newCount=((Number)newMetrics.get(metric)).longValue();
   rows.add(Map.of("type",rule.get("type"),"columns",rule.get("columns"),"metric",metric,"before",oldCount,"after",newCount,"reducedBy",oldCount-newCount,"beforeMetrics",oldMetrics,"afterMetrics",newMetrics,"params",rule.getOrDefault("params",Map.of())));
  }
  return rows;
 }
}
