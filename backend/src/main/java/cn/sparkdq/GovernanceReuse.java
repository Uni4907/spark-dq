package cn.sparkdq;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.util.*;

/** Reuse persisted successful policies as editable copies, without new templates or writes. */
@Service
public class GovernanceReuse {
 final Tasks tasks;final Datasets ds;
 public GovernanceReuse(Tasks tasks,Datasets ds){this.tasks=tasks;this.ds=ds;}
 static Tasks.GovernanceRequest compatible(Tasks.GovernanceRequest policy,List<String> columns,List<Map<String,Object>> rules){
  var normalized=GovernancePolicies.normalize(policy,columns);
  if(normalized.dropMissing()&&rules.stream().noneMatch(rule->"MISSING".equals(rule.get("type"))))throw Datasets.bad("历史策略包含移除空值，但当前任务没有空值规则");
  if(normalized.deduplicate()&&rules.stream().noneMatch(rule->"DUPLICATE".equals(rule.get("type"))))throw Datasets.bad("历史策略包含业务键去重，但当前任务没有重复规则");
  return normalized;
 }
 @Transactional(readOnly=true,isolation=Isolation.REPEATABLE_READ)
 public Map<String,Object> prepare(String sourceId,String targetId){
  var source=tasks.detail(sourceId);var target=tasks.detail(targetId);
  if(!"SUCCEEDED".equals(source.get("status"))||!(source.get("governance") instanceof Map<?,?> run)||run.get("output_dataset_id")==null)throw new ResponseStatusException(HttpStatus.CONFLICT,"只能复用已成功生成版本的治理策略");
  if(!"SUCCEEDED".equals(target.get("status"))||!"csv-v2".equals(tasks.semantics(target)))throw new ResponseStatusException(HttpStatus.CONFLICT,"请先选择采用当前口径且检测成功的目标任务");
  var data=ds.one(tasks.effectiveDataset(target));Tasks.GovernanceRequest policy;
  try{policy=ds.json.convertValue(((Map<?,?>)source.get("governance")).get("policy"),Tasks.GovernanceRequest.class);}
  catch(IllegalArgumentException error){throw new ResponseStatusException(HttpStatus.CONFLICT,"历史策略无法读取，请选择其他治理任务");}
  @SuppressWarnings("unchecked") var columns=(List<String>)data.get("columns");
  @SuppressWarnings("unchecked") var rules=(List<Map<String,Object>>)target.get("rules");
  return Map.of("sourceTaskId",sourceId,"sourceDatasetName",source.get("dataset_name"),"sourceCreatedAt",source.get("created_at"),"targetDatasetId",data.get("id"),"policy",compatible(policy,columns,rules));
 }
}
