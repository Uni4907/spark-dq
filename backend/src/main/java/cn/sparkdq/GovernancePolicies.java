package cn.sparkdq;
import java.util.*;

/** Canonical policy snapshots; v2/v3 compatibility and v4 ordered business-key retention. */
public final class GovernancePolicies {
 static final Set<String> NORMALIZATIONS=Set.of("TRIM","UPPER","LOWER","TRIM_UPPER","TRIM_LOWER");
 public static Tasks.GovernanceRequest normalize(Tasks.GovernanceRequest request,List<String> columns){
  if(request==null)throw Datasets.bad("缺少治理策略");
  if(request.policyVersion()!=null&&request.policyVersion()!=2&&request.policyVersion()!=3&&request.policyVersion()!=4)throw Datasets.bad("不支持的治理策略版本");
  var fills=validate(request.fills(),columns,true);
  var normalizations=validate(request.normalizations(),columns,false);
  var mappings=mappings(request.mappings(),columns);
  if(!mappings.isEmpty()&&Integer.valueOf(2).equals(request.policyVersion()))throw Datasets.bad("值映射需要治理策略版本 3");
  var order=order(request.deduplicationOrder(),columns);
  if(!order.isEmpty()&&!request.deduplicate())throw Datasets.bad("配置保留顺序时必须启用业务键去重");
  if(!order.isEmpty()&&request.policyVersion()!=null&&request.policyVersion()<4)throw Datasets.bad("保留顺序需要治理策略版本 4");
  if(!request.dropMissing()&&!request.deduplicate()&&fills.isEmpty()&&normalizations.isEmpty()&&mappings.isEmpty())throw Datasets.bad("至少选择一种治理策略");
  int version=!order.isEmpty()||Integer.valueOf(4).equals(request.policyVersion())?4:!mappings.isEmpty()||Integer.valueOf(3).equals(request.policyVersion())?3:2;
  return new Tasks.GovernanceRequest(request.dropMissing(),request.deduplicate(),fills,normalizations,mappings,order,version);
 }
 static List<Tasks.SortSpec> order(List<Tasks.SortSpec> values,List<String> columns){
  if(values==null||values.isEmpty())return List.of();
  if(values.size()>5)throw Datasets.bad("最多配置 5 级保留顺序");
  var seen=new HashSet<String>();
  for(var item:values){
   if(item==null||!columns.contains(item.column())||!seen.add(item.column()))throw Datasets.bad("排序字段必须存在且不能重复");
   if(item.type()==null||!Set.of("TEXT","DECIMAL","DATE").contains(item.type()))throw Datasets.bad("排序类型仅支持文本、精确数值和日期");
   if(item.direction()==null||!Set.of("ASC","DESC").contains(item.direction()))throw Datasets.bad("排序方向必须是 ASC 或 DESC");
   if(item.nulls()==null||!Set.of("FIRST","LAST").contains(item.nulls()))throw Datasets.bad("空值位置必须是 FIRST 或 LAST");
  }
  return List.copyOf(values);
 }
 static Map<String,List<Tasks.ValueMapping>> mappings(Map<String,List<Tasks.ValueMapping>> values,List<String> columns){
  if(values==null||values.isEmpty())return Map.of();
  if(values.size()>columns.size()||!columns.containsAll(values.keySet()))throw Datasets.bad("映射字段必须来自输入数据集");
  var result=new TreeMap<String,List<Tasks.ValueMapping>>();int count=0,characters=0;
  for(var entry:values.entrySet()){
   var pairs=entry.getValue();
   if(pairs==null||pairs.isEmpty()||pairs.size()>50)throw Datasets.bad("每个字段须配置 1 至 50 条映射");
   var sorted=new TreeMap<String,Tasks.ValueMapping>();
   for(var pair:pairs){
    if(pair==null||pair.from()==null||pair.to()==null)throw Datasets.bad("映射来源和目标必须是字符串，不能为 NULL");
    for(String text:List.of(pair.from(),pair.to()))
     if(text.length()>1000||text.indexOf(0)>=0||text.equals("\\N")||!java.nio.charset.StandardCharsets.UTF_8.newEncoder().canEncode(text))throw Datasets.bad("映射文本须是有效 Unicode、最多 1000 字符，不能包含 NUL 或保留空值标记");
    if(sorted.putIfAbsent(pair.from(),pair)!=null)throw Datasets.bad("同一字段的映射来源不能重复");
    count++;characters+=pair.from().length()+pair.to().length();
   }
   result.put(entry.getKey(),List.copyOf(sorted.values()));
  }
  if(count>200||characters>40000)throw Datasets.bad("每次治理最多 200 条映射，来源和目标合计最多 40000 字符");
  return Collections.unmodifiableMap(result);
 }
 static Map<String,Object> validate(Map<String,Object> values,List<String> columns,boolean fill){
  if(values==null)return Map.of();
  if(values.size()>columns.size()||!columns.containsAll(values.keySet()))throw Datasets.bad("治理字段必须来自输入数据集");
  var result=new TreeMap<String,Object>();
  for(var entry:values.entrySet()){
   if(!(entry.getValue() instanceof String text))throw Datasets.bad("治理配置值必须为字符串");
   if(fill){
    if(text.isBlank()||text.length()>1000||text.indexOf(0)>=0||text.equals("\\N"))throw Datasets.bad("填充值须非空白、不超过 1000 字符，且不能是保留空值标记或包含 NUL");
   }else if(!NORMALIZATIONS.contains(text))throw Datasets.bad("不支持的文本标准化操作");
   result.put(entry.getKey(),text);
  }
  return Collections.unmodifiableMap(result);
 }
}
