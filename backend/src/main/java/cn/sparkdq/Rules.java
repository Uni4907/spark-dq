package cn.sparkdq;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Pattern;

/** Validated immutable definitions shared by templates, submissions and snapshots. */
public final class Rules {
 public record Spec(String type,List<String> columns,Map<String,Object> params,Integer definitionVersion){
  public Spec(String type,List<String> columns,Map<String,Object> params){this(type,columns,params,null);}
 }
 static final Set<String> TYPES=Set.of("MISSING","DUPLICATE","TYPE","RANGE","REGEX","IQR","FD");
 public static List<Map<String,Object>> normalize(List<Spec> specs,List<String> available){
  if(specs==null||specs.isEmpty()||specs.size()>101)throw Datasets.bad("规则数量须为 1～101");
  var out=new ArrayList<Map<String,Object>>();var unique=new HashSet<String>();int duplicates=0;
  for(Spec spec:specs){
   if(spec==null||spec.type()==null||!TYPES.contains(spec.type()))throw Datasets.bad("不支持的规则类型");
   if(spec.definitionVersion()!=null&&spec.definitionVersion()!=1)throw Datasets.bad("不支持的规则定义版本");
   var columns=spec.columns();
   if(columns==null||columns.isEmpty()||columns.size()>100||new HashSet<>(columns).size()!=columns.size()
    ||columns.stream().anyMatch(c->c==null||!c.matches("[A-Za-z][A-Za-z0-9_]{0,63}"))
    ||(available!=null&&!available.containsAll(columns)))throw Datasets.bad("规则字段须唯一且来自数据集");
   if(!Set.of("DUPLICATE","FD").contains(spec.type())&&columns.size()!=1)throw Datasets.bad("此规则只能配置一个字段");
   if("DUPLICATE".equals(spec.type())&&++duplicates>1)throw Datasets.bad("每个任务最多配置一个重复规则");
   Map<String,Object> p=spec.params()==null?Map.of():spec.params();var params=new TreeMap<String,Object>();
   switch(spec.type()){
    case "MISSING","DUPLICATE" -> {if(!p.isEmpty())throw Datasets.bad("空值和重复规则不接受额外参数");}
    case "TYPE" -> {
     if(!p.keySet().equals(Set.of("dataType"))||!(p.get("dataType") instanceof String dataType)||!Set.of("INTEGER","DECIMAL","DATE").contains(dataType))throw Datasets.bad("类型须为 INTEGER、DECIMAL 或 DATE");
     params.put("dataType",p.get("dataType"));
    }
    case "RANGE" -> {
     if(p.isEmpty()||!Set.of("min","max").containsAll(p.keySet()))throw Datasets.bad("范围须提供 min 或 max");
     for(String key:p.keySet()){
      Object value=p.get(key);if(!(value instanceof Number)&&!(value instanceof String text&&text.length()<=64&&text.matches("[+-]?(?:[0-9]+(?:\\.[0-9]+)?|\\.[0-9]+)")))throw Datasets.bad("范围边界必须为数字或十进制字符串");
      BigDecimal number;
      try{number=new BigDecimal(value.toString()).stripTrailingZeros();}catch(Exception e){throw Datasets.bad("范围边界必须为有限数字");}
      if(number.scale()>10||number.precision()-number.scale()>28)throw Datasets.bad("范围边界最多 28 位整数、10 位小数");
      params.put(key,number.toPlainString());
     }
     if(params.containsKey("min")&&params.containsKey("max")&&new BigDecimal(params.get("min").toString()).compareTo(new BigDecimal(params.get("max").toString()))>0)throw Datasets.bad("min 不能大于 max");
    }
    case "FD" -> {
     if(columns.size()>16||!p.keySet().equals(Set.of("dependentColumns"))||!(p.get("dependentColumns") instanceof List<?> ys)||ys.isEmpty()||ys.size()>16||new HashSet<>(ys).size()!=ys.size()
       ||ys.stream().anyMatch(v->!(v instanceof String text)||!text.matches("[A-Za-z][A-Za-z0-9_]{0,63}")||columns.contains(v))
       ||(available!=null&&!available.containsAll(ys)))throw Datasets.bad("FD的X/Y须各1～16个唯一字段，互不重叠且来自数据集");
     params.put("dependentColumns",List.copyOf(ys));
    }
    case "IQR" -> {
     if(!Set.of("k").containsAll(p.keySet()))throw Datasets.bad("IQR仅接受k参数");
     Object v=p.getOrDefault("k","1.5");String t=String.valueOf(v);
     if(t.length()>16||!t.matches("[0-9]+(?:\\.[0-9]{1,4})?"))throw Datasets.bad("IQR k须为(0,10]内最多4位小数");
     BigDecimal k=new BigDecimal(t);if(k.signum()<=0||k.compareTo(BigDecimal.TEN)>0)throw Datasets.bad("IQR k须在(0,10]内");
     params.put("k",k.stripTrailingZeros().toPlainString());
    }
    case "REGEX" -> {
     if(!p.keySet().equals(Set.of("pattern"))||!(p.get("pattern") instanceof String pattern)||pattern.isEmpty()||pattern.length()>256)throw Datasets.bad("正则须为 1～256 字符");
     try{Pattern.compile("\\A(?:"+pattern+")\\z");}catch(Exception e){throw Datasets.bad("正则表达式语法错误");}
     params.put("pattern",pattern);
    }
   }
   String signature=spec.type()+columns+params;
   if(!unique.add(signature))throw Datasets.bad("不能重复配置相同规则");
   out.add(Map.of("type",spec.type(),"columns",List.copyOf(columns),"params",Collections.unmodifiableMap(params),"definitionVersion",1));
  }
  return out;
 }
 public static List<Map<String,Object>> withIds(List<Map<String,Object>> definitions){
  var out=new ArrayList<Map<String,Object>>();
  for(var definition:definitions){var row=new LinkedHashMap<>(definition);row.put("id",Datasets.id());out.add(row);}
  return out;
 }
 @SuppressWarnings("unchecked") public static List<Spec> stored(List<?> definitions){
  var specs=new ArrayList<Spec>();
  for(var definition:definitions){
   var r=(Map<String,Object>)definition;
   specs.add(new Spec((String)r.get("type"),(List<String>)r.get("columns"),(Map<String,Object>)r.get("params"),r.get("definitionVersion")==null?null:((Number)r.get("definitionVersion")).intValue()));
  }
  return specs;
 }
}
