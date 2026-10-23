package cn.sparkdq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

class ReportDocumentsTest {
 final ObjectMapper json=new ObjectMapper();
 Map<String,Object> rule(){return Map.of("type","RANGE","columns",List.of("amount"),"params",Map.of("min","0","max","100"),"metrics",Map.of("invalidCount",3,"checkedCount",8,"skippedBlankCount",2));}
 Map<String,Object> task(){return new HashMap<>(Map.of("id","task-1","status","SUCCEEDED","dataset_name","中文数据.csv","dataset_id","data-1","total_rows",10,"duration_ms",1234,"spark_version","3.5.8","rules",List.of(rule()),"snapshot",Map.of("input_sha256","abc123","semantics_version","csv-v2")));}
 Map<String,Object> report(Map<String,Object> task){return new HashMap<>(Map.of("task",task,"note","各规则异常可能重叠，不合计为总异常行数。"));}
 String html(Map<String,Object> report){return ReportDocuments.render(json.valueToTree(report));}
 @Test void successShowsExactPersistedCountsAndSnapshotWithoutInventedScore(){
  var t=task();t.put("file_path","/private/server/input.csv");String html=html(report(t));assertTrue(html.contains("中文数据.csv"));assertTrue(html.contains("异常行数：3"));assertTrue(html.contains("已检查行数：8"));assertTrue(html.contains("abc123"));assertTrue(html.contains("csv-v2"));assertTrue(html.contains("1234 毫秒"));assertFalse(html.contains("/private/server/"));assertFalse(html.contains("质量总分"));assertTrue(html.contains("@media print"));assertFalse(html.contains("<script"));
 }
 @Test void allUserTextAndRuleParametersAreEscaped(){
  var t=task();t.put("dataset_name","<img src=x onerror=alert(1)> & \"文件\"");t.put("rules",List.of(Map.of("type","REGEX","columns",List.of("<field>"),"params",Map.of("pattern","</td><script>bad()</script>"),"metrics",Map.of("invalidCount",1))));
  String html=html(report(t));assertFalse(html.contains("<img"));assertFalse(html.contains("<script>"));assertFalse(html.contains("<field>"));assertTrue(html.contains("&lt;img"));assertTrue(html.contains("&amp; &quot;文件&quot;"));assertTrue(html.contains("&lt;/td&gt;&lt;script&gt;"));
 }
 @Test void failedAndCancelledNeverRenderSuccessStatisticsEvenIfStaleFieldsExist(){
  for(String status:List.of("FAILED","CANCELLED")){var t=task();t.put("status",status);t.put("error","<timeout>\n详细原因");var r=report(t);r.put("comparison",List.of(Map.of("type","RANGE","columns",List.of("amount"),"before",2,"after",0,"reducedBy",2)));String html=html(r);assertTrue(html.contains("任务处理记录"));assertTrue(html.contains("&lt;timeout&gt;"));assertTrue(html.contains("未产生成功的质量统计"));assertFalse(html.contains("<h2>检测结果</h2>"));assertFalse(html.contains("之前异常"));assertFalse(html.contains("异常行数：3"));}
 }
 @Test void worseningAndUnavailableComparisonAreHonest(){
  var r=report(task());r.put("sourceTaskId","parent");r.put("beforeRows",10);r.put("afterRows",8);r.put("comparison",List.of(Map.of("type","RANGE","columns",List.of("amount"),"params",Map.of("min","0"),"before",2,"after",3,"reducedBy",-1)));assertTrue(html(r).contains("<td>-1</td>"));r.remove("comparison");r.put("comparisonUnavailable","来源口径不同");assertTrue(html(r).contains("前后对比不可用：来源口径不同"));
 }
 @Test void governanceIncludesPolicyActualAuditAndVersionWithoutUnescapedMapping(){
  var t=task();t.put("governance",Map.of("input_rows",10,"output_rows",8,"removed_rows",2,"output_dataset_id","version-2","policy",Map.of("policyVersion",4,"fills",Map.of("name","<filled>"),"mappings",Map.of("status",List.of(Map.of("from","","to","<ACTIVE>"))),"deduplicate",true,"deduplicationOrder",List.of(Map.of("column","score","type","DECIMAL","direction","DESC","nulls","LAST"))),"audit",Map.of("transformations",List.of(Map.of("column","status","operation","VALUE_MAPPING","matchedRows",4,"changedRows",3,"entries",List.of(Map.of("from","","to","<ACTIVE>","matchedRows",4,"changedRows",3)))),"deduplicatedRows",2,"droppedMissingRows",0)));
  var r=report(t);r.put("outputDataset",Map.of("sha256","output-sha"));String html=html(r);assertTrue(html.contains("版本SHA-256"));assertTrue(html.contains("output-sha"));assertTrue(html.contains("（空字符串） → &lt;ACTIVE&gt;"));assertTrue(html.contains("匹配 4，改变 3"));assertTrue(html.contains("精确数值 / 降序 / 空值最后"));assertFalse(html.contains("<ACTIVE>"));
 }
 @Test void missingHistoricalSnapshotOrAuditIsNotReplacedWithZero(){
  var t=task();t.remove("snapshot");t.put("governance",Map.of("policy",Map.of("deduplicate",true)));String html=html(report(t));assertTrue(html.contains("此历史任务未记录输入快照"));assertTrue(html.contains("此历史治理未记录操作级审计"));assertTrue(html.contains("未记录"));
 }
 @Test void htmlEndpointUsesUtf8SafeHeadersAndExplicitDownload()throws Exception{
  Tasks tasks=mock(Tasks.class);Reports reports=mock(Reports.class);when(reports.report("task-1")).thenReturn(report(task()));var mvc=MockMvcBuilders.standaloneSetup(new TaskApi(tasks,reports,new ReportDocuments(reports,json))).build();
  mvc.perform(get("/api/tasks/task-1/report.html")).andExpect(status().isOk()).andExpect(content().contentType("text/html;charset=UTF-8")).andExpect(header().string("Content-Disposition","inline; filename=quality-report.html")).andExpect(header().string("X-Content-Type-Options","nosniff")).andExpect(header().string("Cache-Control","no-store")).andExpect(header().exists("Content-Security-Policy"));
  mvc.perform(get("/api/tasks/task-1/report.html?download=true")).andExpect(status().isOk()).andExpect(header().string("Content-Disposition","attachment; filename=quality-report.html"));
 }
 @Test void unfinishedOrUnknownTasksKeepOriginalHttpErrors()throws Exception{
  Tasks tasks=mock(Tasks.class);Reports reports=mock(Reports.class);when(reports.report("pending")).thenThrow(new ResponseStatusException(HttpStatus.CONFLICT));when(reports.report("unknown")).thenThrow(new ResponseStatusException(HttpStatus.NOT_FOUND));var mvc=MockMvcBuilders.standaloneSetup(new TaskApi(tasks,reports,new ReportDocuments(reports,json))).build();mvc.perform(get("/api/tasks/pending/report.html")).andExpect(status().isConflict());mvc.perform(get("/api/tasks/unknown/report.html")).andExpect(status().isNotFound());
 }
}
