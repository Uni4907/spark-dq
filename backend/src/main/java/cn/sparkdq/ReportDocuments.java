package cn.sparkdq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import java.util.*;

/** Offline, printable presentation of the same persisted report as the JSON API. */
@Service
public class ReportDocuments {
 final Reports reports;final ObjectMapper json;
 public ReportDocuments(Reports reports,ObjectMapper json){this.reports=reports;this.json=json;}
 public String document(String id){return render(json.valueToTree(reports.report(id)));}
 static String escape(String value){return value.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace("\"","&quot;").replace("'","&#39;");}
 static String text(JsonNode node){return node==null||node.isMissingNode()||node.isNull()?"未记录":node.asText();}
 static String cell(JsonNode node){return escape(text(node));}
 static String label(String value){return switch(value){
  case "SUCCEEDED"->"已完成";case "FAILED"->"失败";case "CANCELLED"->"已取消";
  case "MISSING"->"空值";case "DUPLICATE"->"重复";case "TYPE"->"类型";case "RANGE"->"数值范围";case "REGEX"->"正则格式";case "IQR"->"IQR数值异常";case "FD"->"函数依赖冲突";
  case "nullCount"->"缺失值";case "emptyCount"->"空字符串";case "whitespaceCount"->"纯空格";case "blankCount"->"空值合计";
  case "duplicateGroups"->"重复组数";case "duplicateRows"->"涉及重复行数";case "excessRows"->"多余行数";
  case "checkedCount"->"已检查行数";case "invalidCount"->"异常行数";case "skippedBlankCount"->"跳过空值行数";case "nonNumericCount"->"非数值行数";
  case "checkedGroups"->"检查组数";case "conflictGroups"->"冲突组数";case "conflictRows"->"冲突涉及行数";case "skippedNullCount"->"含NULL跳过行数";case "skippedEmptyCount"->"含空串跳过行数";case "skippedWhitespaceCount"->"含纯空格跳过行数";
  case "q1"->"第一四分位数Q1";case "q3"->"第三四分位数Q3";case "iqr"->"四分位距";case "k"->"倍数k";case "lowerBound"->"下界";case "upperBound"->"上界";
  case "status"->"计算状态";case "algorithmVersion"->"算法版本";case "numericDomain"->"数值域";case "minimumValidCount"->"最少有效样本";case "evaluatedCount"->"参与判定行数";case "skippedCount"->"跳过行数";case "outOfDomainCount"->"超精度域行数";
  case "TRIM"->"去除首尾空格";case "UPPER"->"转大写";case "LOWER"->"转小写";case "TRIM_UPPER"->"去空格并转大写";case "TRIM_LOWER"->"去空格并转小写";
  case "FILL_MISSING"->"填充缺失值";case "VALUE_MAPPING"->"精确值映射";
  case "CSV_SINGLE"->"原始CSV";case "CSV"->"分片CSV执行副本";case "PARQUET"->"Parquet执行副本";
  case "INTEGER"->"64位整数";case "DECIMAL"->"精确数值";case "DATE"->"日期";case "TEXT"->"文本";
  case "ASC"->"升序";case "DESC"->"降序";case "FIRST"->"空值最前";case "LAST"->"空值最后";
  default->value;
 };}
 static String columns(JsonNode rule){var names=new ArrayList<String>();rule.path("columns").forEach(n->names.add(text(n)));return escape(String.join(" + ",names));}
 static String definition(JsonNode rule){
  var p=rule.path("params");String value=switch(rule.path("type").asText()){
   case "TYPE"->label(p.path("dataType").asText());
   case "RANGE"->"包含边界："+(p.hasNonNull("min")?text(p.get("min")):"不限")+" ～ "+(p.hasNonNull("max")?text(p.get("max")):"不限");
   case "FD"->"X→Y（精确原文/空白跳过）："+rule.path("columns").toString()+" → "+p.path("dependentColumns").toString()+"；全部冲突参与行，不自动修复";
   case "IQR"->"k="+text(p.path("k"))+"；精确十进制连续四分位数，至少4有效值，每次复检重新拟合";
   case "REGEX"->"整字段匹配："+text(p.path("pattern"));default->"";};return escape(value);
 }
 static String iqrSummary(JsonNode metrics){return "Q1="+cell(metrics.path("q1"))+"，Q3="+cell(metrics.path("q3"))+"，下界="+cell(metrics.path("lowerBound"))+"，上界="+cell(metrics.path("upperBound"))+"，有效="+cell(metrics.path("checkedCount"))+"，跳过="+cell(metrics.path("skippedCount"))+"，异常="+cell(metrics.path("invalidCount"));}
 static String literal(JsonNode node){return escape(node==null||node.isNull()?"NULL":node.isTextual()&&node.asText().isEmpty()?"（空字符串）":node.asText());}
 static void pair(StringBuilder out,String name,JsonNode value){out.append("<dt>").append(escape(name)).append("</dt><dd>").append(cell(value)).append("</dd>");}
 static void heading(StringBuilder out,String title){out.append("<h2>").append(title).append("</h2>");}
 static String render(JsonNode report){
  var t=report.path("task");boolean success=t.path("status").asText().equals("SUCCEEDED");var out=new StringBuilder();
  out.append("<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>SparkDQ · ").append(success?"数据质量报告":"任务处理记录").append(" · ").append(cell(t.path("id"))).append("</title><style>")
   .append("*{box-sizing:border-box}body{margin:0;background:#eef3f4;color:#19323d;font:14px/1.7 system-ui,Microsoft YaHei,sans-serif}main{max-width:1040px;margin:28px auto;padding:40px;background:white;border:1px solid #dce6e9;border-radius:12px}h1{font-size:30px;margin:6px 0}h2{font-size:19px;margin-top:32px;border-bottom:2px solid #167d8d;padding-bottom:6px}h3{font-size:15px}p{margin:8px 0}.brand{color:#167d8d;font-weight:700}.hint{color:#607984}.notice{overflow-wrap:anywhere;white-space:pre-wrap;border-left:4px solid #167d8d;background:#eff8f8;padding:12px 16px}dl{display:grid;grid-template-columns:130px minmax(0,1fr);gap:6px 16px}dt{color:#607984}dd{margin:0;overflow-wrap:anywhere}table{border-collapse:collapse;width:100%;margin:14px 0;font-size:13px}th,td{text-align:left;border:1px solid #dce6e9;padding:10px;vertical-align:top;white-space:pre-wrap;overflow-wrap:anywhere}th{background:#f2f7f7}small{display:block;color:#607984}li{white-space:pre-wrap;overflow-wrap:anywhere}.error{white-space:pre-wrap;border:1px solid #e8c7c7;padding:14px;color:#8b3333}.status{font-weight:700}.scroll{overflow-x:auto}.print-hint{padding:12px 16px;background:#e0eff2;border-radius:6px}footer{border-top:1px solid #dce6e9;margin-top:30px;padding-top:12px;color:#607984}@media(max-width:640px){main{margin:0;padding:20px;border:0;border-radius:0}dl{grid-template-columns:100px minmax(0,1fr)}}@media print{@page{size:A4;margin:15mm}body{background:white;font-size:11px}main{margin:0;padding:0;max-width:none;border:0}.print-hint{display:none}h2,h3{break-after:avoid}tr,dl,.notice{break-inside:avoid}thead{display:table-header-group}.scroll{overflow:visible}th,td{padding:6px}h1{font-size:25px}}")
   .append("</style></head><body><main><div class=\"print-hint\">使用浏览器“打印”（Ctrl+P），可选择打印机或“另存为PDF”。本报告为独立文件，可离线阅读。</div><p class=\"brand\">SparkDQ · 数据质量检测与治理</p><h1>")
   .append(success?"数据质量报告":"任务处理记录").append("</h1><p class=\"status\">任务状态：").append(escape(label(t.path("status").asText()))).append("</p><dl>");
  pair(out,"数据集",t.path("dataset_name"));pair(out,"任务编号",t.path("id"));pair(out,"数据集编号",t.path("dataset_id"));pair(out,"创建时间",t.path("created_at"));pair(out,"开始时间",t.path("started_at"));pair(out,"结束时间",t.path("finished_at"));
  if(t.path("duration_ms").isNumber())out.append("<dt>任务耗时</dt><dd>").append(t.path("duration_ms").asLong()).append(" 毫秒（包含准备与结果登记等开销）</dd>");
  out.append("</dl><p class=\"notice\">").append(cell(report.path("note"))).append("</p>");
  if(!success){out.append("<p>此任务未产生成功的质量统计，不作为数据质量合格结论。</p>");if(t.hasNonNull("error"))out.append("<h2>处理说明</h2><p class=\"error\">").append(cell(t.get("error"))).append("</p>");}
  if(success){
   heading(out,"检测结果");out.append("<p>检测记录：<strong>").append(cell(t.path("total_rows"))).append("</strong> 行 · Spark ").append(cell(t.path("spark_version"))).append("</p><div class=\"scroll\"><table><thead><tr><th>规则与字段</th><th>规则配置</th><th>实际统计</th></tr></thead><tbody>");
   for(var rule:t.path("rules")){
    out.append("<tr><td>").append(escape(label(rule.path("type").asText()))).append(" · ").append(columns(rule)).append("</td><td>").append(definition(rule)).append("</td><td>");
    if(!rule.path("metrics").isObject())out.append("未记录");else rule.path("metrics").fields().forEachRemaining(entry->out.append(escape(label(entry.getKey()))).append("：").append(cell(entry.getValue())).append("<br>"));out.append("</td></tr>");
   }out.append("</tbody></table></div>");
   if(report.path("comparison").isArray()&&!report.path("comparison").isEmpty()){
    for(var row:report.path("comparison"))if("IQR".equals(row.path("type").asText()))out.append("<p class=\"notice\">IQR 每次重新拟合；前次：").append(iqrSummary(row.path("beforeMetrics"))).append("；本次：").append(iqrSummary(row.path("afterMetrics"))).append("。异常数下降不等于修复。</p>");
    heading(out,"治理或复检前后对比");out.append("<p>来源任务 ").append(cell(report.path("sourceTaskId"))).append(" · ").append(cell(report.path("beforeRows"))).append(" 行 → ").append(cell(report.path("afterRows"))).append(" 行。异常减少为负数表示增加，不隐藏恶化。</p><table><thead><tr><th>规则与配置</th><th>之前异常</th><th>之后异常</th><th>减少</th></tr></thead><tbody>");
    for(var row:report.path("comparison"))out.append("<tr><td>").append(escape(label(row.path("type").asText()))).append(" · ").append(columns(row)).append("<small>").append(definition(row)).append("</small></td><td>").append(cell(row.path("before"))).append("</td><td>").append(cell(row.path("after"))).append("</td><td>").append(cell(row.path("reducedBy"))).append("</td></tr>");out.append("</tbody></table>");
   }else if(report.hasNonNull("comparisonUnavailable"))out.append("<p class=\"hint\">前后对比不可用：").append(cell(report.get("comparisonUnavailable"))).append("</p>");
   governance(out,t.path("governance"),report.path("outputDataset"));
  }
  heading(out,"规则与输入追溯");var snapshot=t.path("snapshot");
  if(snapshot.isObject()){
   out.append("<dl>");pair(out,"统计口径",snapshot.path("semantics_version"));pair(out,"原始输入SHA-256",snapshot.path("input_sha256"));
   out.append("<dt>执行输入</dt><dd>").append(escape(label(snapshot.path("executionInput").path("format").asText("CSV_SINGLE")))).append("</dd>");
   if(snapshot.hasNonNull("input_manifest_sha256"))pair(out,"副本清单SHA-256",snapshot.get("input_manifest_sha256"));if(snapshot.hasNonNull("parent_task_id"))pair(out,"来源任务",snapshot.get("parent_task_id"));out.append("</dl>");
  }else out.append("<p>此历史任务未记录输入快照，不补造追溯信息。</p>");
  if(!success){out.append("<h3>已配置规则</h3><ul>");for(var rule:t.path("rules"))out.append("<li>").append(escape(label(rule.path("type").asText()))).append(" · ").append(columns(rule)).append(" ").append(definition(rule)).append("</li>");out.append("</ul>");}
  out.append("<p class=\"hint\">本报告仅包含已配置规则的结果，不给出未检测字段的合格承诺。各规则异常可能重叠，不能相加为不同异常记录数。</p><footer>SparkDQ · 报告文档 v1 · 数据源为持久化任务与治理审计；原始数据保留。</footer></main></body></html>");return out.toString();
 }
 static void governance(StringBuilder out,JsonNode run,JsonNode output){
  if(!run.isObject())return;heading(out,"治理策略与审计");var policy=run.path("policy");
  out.append("<dl>");pair(out,"策略版本",policy.path("policyVersion"));pair(out,"治理输入行数",run.path("input_rows"));pair(out,"治理输出行数",run.path("output_rows"));pair(out,"移除行数",run.path("removed_rows"));pair(out,"生成版本",run.path("output_dataset_id"));if(output.hasNonNull("sha256"))pair(out,"版本SHA-256",output.get("sha256"));out.append("</dl><ul>");
  policy.path("normalizations").fields().forEachRemaining(entry->out.append("<li>标准化 ").append(escape(entry.getKey())).append("：").append(escape(label(entry.getValue().asText()))).append("</li>"));
  policy.path("fills").fields().forEachRemaining(entry->out.append("<li>缺失填充 ").append(escape(entry.getKey())).append("：").append(literal(entry.getValue())).append("</li>"));
  policy.path("mappings").fields().forEachRemaining(entry->{for(var pair:entry.getValue())out.append("<li>精确映射 ").append(escape(entry.getKey())).append("：").append(literal(pair.path("from"))).append(" → ").append(literal(pair.path("to"))).append("</li>");});
  out.append("<li>移除仍缺失记录：").append(policy.path("dropMissing").asBoolean()?"是":"否").append("</li><li>业务键去重：").append(policy.path("deduplicate").asBoolean()?"是":"否").append("</li>");
  for(var order:policy.path("deduplicationOrder"))out.append("<li>保留优先级：").append(cell(order.path("column"))).append(" / ").append(escape(label(order.path("type").asText()))).append(" / ").append(escape(label(order.path("direction").asText()))).append(" / ").append(escape(label(order.path("nulls").asText()))).append("</li>");out.append("</ul>");
  var audit=run.path("audit");if(!audit.isObject()){out.append("<p>此历史治理未记录操作级审计，不补造改变行数。</p>");return;}
  if(audit.path("transformations").isArray()&&!audit.path("transformations").isEmpty()){
   out.append("<table><thead><tr><th>字段</th><th>操作</th><th>匹配行数</th><th>实际改变行数</th></tr></thead><tbody>");
   for(var change:audit.path("transformations")){
    out.append("<tr><td>").append(cell(change.path("column"))).append("</td><td>").append(escape(label(change.path("operation").asText())));
    for(var entry:change.path("entries"))out.append("<small>").append(literal(entry.path("from"))).append(" → ").append(literal(entry.path("to"))).append("；匹配 ").append(cell(entry.path("matchedRows"))).append("，改变 ").append(cell(entry.path("changedRows"))).append("</small>");
    out.append("</td><td>").append(cell(change.path("matchedRows"))).append("</td><td>").append(cell(change.path("changedRows"))).append("</td></tr>");
   }out.append("</tbody></table><p class=\"hint\">按每一步当时的数据统计；同一记录可能被多步改变，改变行数不能相加为不同记录数。</p>");
  }
  out.append("<dl>");pair(out,"移除仍缺失记录",audit.path("droppedMissingRows"));pair(out,"去重移除记录",audit.path("deduplicatedRows"));out.append("</dl>");
 }
}
