<script setup>
import IqrComparison from './IqrComparison.vue';
import {ref,computed,watch,onMounted,onUnmounted,nextTick} from 'vue';
import RuleManager from './RuleManager.vue';
import GovernancePanel from './GovernancePanel.vue';
import ImportPanel from './ImportPanel.vue';
import MysqlImportPanel from './MysqlImportPanel.vue';
import ControlledFilesPanel from './ControlledFilesPanel.vue';
import DatasetSource from './DatasetSource.vue';
import ExecutionPanel from './ExecutionPanel.vue';
import AnomalyChart from './AnomalyChart.vue';
import ReportActions from './ReportActions.vue';
import DatasetHistory from './DatasetHistory.vue';
import {stableValue,isActive,pollDelay,pollingLoop,settleRequests} from './refresh-state.mjs';
import {serviceView} from './execution-view.mjs';
const datasets=ref([]),tasks=ref([]),selected=ref(''),dataset=ref(null),fields=ref([]),keys=ref([]),detail=ref(null),samples=ref(null),error=ref(''),pollError=ref(''),notice=ref(''),busy=ref(false);
const report=ref(null),rulePlan=ref(null),managerBusy=ref(false),activeTaskId=ref('');
const executionFormat=ref('CSV_SINGLE');
const executionService=ref(null),executionServiceView=computed(()=>serviceView(executionService.value));
const canStartTasks=computed(()=>!executionServiceView.value.blocked);
let serviceSequence=0;async function refreshExecutionService(){const seq=++serviceSequence;const result=await api('/execution/status');if(!disposed&&seq===serviceSequence)executionService.value=stableValue(executionService.value,result);}
const inputLabels={CSV_SINGLE:'原始 CSV',CSV:'分片 CSV 执行副本',PARQUET:'Parquet 执行副本'};
const PAGE_SIZE=20;
const datasetReady=ref(false),taskReady=ref(false);
const datasetPage=ref(1),datasetTotal=ref(0),datasetPages=ref(0),datasetSearch=ref(''),datasetQuery=ref(''),datasetPageLoading=ref(false),datasetLoading=ref(false);
const taskPage=ref(1),taskTotal=ref(0),taskPages=ref(0),taskStatus=ref('ALL'),taskPageLoading=ref(false),detailLoading=ref(false);
const selectedOutsidePage=computed(()=>selected.value&&!datasets.value.some(d=>d.id===selected.value));
const typeLabels={MISSING:'空值',DUPLICATE:'重复',TYPE:'类型',RANGE:'数值范围',REGEX:'正则格式',IQR:'IQR数值异常',FD:'函数依赖冲突'};
const baseRules=computed(()=>[...fields.value.map(c=>({type:'MISSING',columns:[c],params:{}})),{type:'DUPLICATE',columns:[...keys.value],params:{}}]);
function configText(rule){const p=rule.params||{};if(rule.type==='TYPE')return {INTEGER:'64 位整数',DECIMAL:'十进制数',DATE:'日期 YYYY-MM-DD'}[p.dataType]||p.dataType;if(rule.type==='RANGE')return '包含边界：'+(p.min??'不限')+' ～ '+(p.max??'不限');if(rule.type==='FD')return rule.columns.join(' + ')+' → '+(p.dependentColumns||[]).join(' + ')+'；精确原文，空白跳过，保留全部冲突参与行';if(rule.type==='IQR')return 'IQR · k='+(p.k??'1.5')+' · 精确十进制线性插值，每次复检重新拟合';if(rule.type==='REGEX')return '整字段匹配：'+p.pattern;return '';}
const operationLabels={TRIM:'去除首尾空格',UPPER:'转大写',LOWER:'转小写',TRIM_UPPER:'去空格并转大写',TRIM_LOWER:'去空格并转小写',FILL_MISSING:'填充缺失值',VALUE_MAPPING:'精确值映射'};
function policyText(policy){const parts=[];for(const [field,action] of Object.entries(policy.normalizations||{}))parts.push(field+'：'+operationLabels[action]);for(const [field,pairs] of Object.entries(policy.mappings||{}))parts.push(field+'：映射 '+pairs.length+' 项');for(const [field,value] of Object.entries(policy.fills||{}))parts.push(field+'：缺失时填充为“'+value+'”');if(policy.dropMissing)parts.push('移除仍缺失的记录');if(policy.deduplicate)parts.push('按业务键去重');if(policy.deduplicationOrder?.length)parts.push('保留顺序：'+retentionText(policy.deduplicationOrder));return parts.join('；');}
function retentionText(order){return order.map((item,i)=>(i+1)+'. '+item.column+' / '+({TEXT:'文本',DECIMAL:'精确数值',DATE:'日期'}[item.type]||item.type)+' / '+(item.direction==='DESC'?'降序':'升序')+' / '+(item.nulls==='FIRST'?'空值最前':'空值最后')).join('；');}
function literalText(value){return value===''?'（空字符串）':value.trim()===''?'（空白文本，'+value.length+' 个字符）':'“'+value+'”';}
let disposed=false,datasetRequesting=false,taskRequesting=false,datasetPageSequence=0,taskPageSequence=0,datasetSequence=0,detailSequence=0,previewSequence=0;
const labels={QUEUED:'排队中',RUNNING:'检测中',CANCELLING:'正在停止',CANCELLED:'已取消',SUCCEEDED:'已完成',FAILED:'失败'};
async function api(path,options={}){const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),30000);try{const response=await fetch('/api'+path,{...options,signal:controller.signal});let body;try{body=await response.json();}catch{throw new Error(response.ok?'服务返回的数据不完整，请刷新后重试':'服务暂时不可用，请稍后重试');}if(!response.ok)throw new Error(body.message||'请求失败');return body;}catch(e){if(e.name==='AbortError')throw new Error('请求超时，请稍后重试');if(e instanceof TypeError)throw new Error('暂时无法连接服务，请稍后重试');throw e;}finally{clearTimeout(timeout);}}
async function guarded(fn,clear=true){if(clear)error.value='';try{await fn();if(!clear)pollError.value='';}catch(e){if(!disposed){if(clear)error.value=e.message;else pollError.value=e.message;}}}
// Independent sequences keep slow polling responses from replacing a newer page or filter.
async function fetchDatasetPage(force=false){
 if(datasetRequesting&&!force)return;
 const sequence=++datasetPageSequence,page=datasetPage.value,q=datasetQuery.value;let clampedPage=null;datasetRequesting=true;datasetPageLoading.value=!datasetReady.value||force;
 const current=()=>!disposed&&sequence===datasetPageSequence&&page===datasetPage.value&&q===datasetQuery.value;
 try{const result=await api('/datasets/page?'+new URLSearchParams({page,pageSize:PAGE_SIZE,q}));if(!current())return;
  datasets.value=stableValue(datasets.value,result.items);datasetReady.value=true;datasetTotal.value=result.total;datasetPages.value=result.totalPages;
  if(result.totalPages>0&&page>result.totalPages)clampedPage=result.totalPages;
 }catch(e){if(current())throw e;}finally{if(sequence===datasetPageSequence){datasetPageLoading.value=false;datasetRequesting=false;}}
 if(clampedPage!==null&&current()){datasetPage.value=clampedPage;await fetchDatasetPage(true);}
}
async function fetchTaskPage(force=false){
 if(taskRequesting&&!force)return;
 const sequence=++taskPageSequence,page=taskPage.value,status=taskStatus.value;let clampedPage=null;taskRequesting=true;taskPageLoading.value=!taskReady.value||force;
 const current=()=>!disposed&&sequence===taskPageSequence&&page===taskPage.value&&status===taskStatus.value;
 try{const result=await api('/tasks/page?'+new URLSearchParams({page,pageSize:PAGE_SIZE,status}));if(!current())return;
  tasks.value=stableValue(tasks.value,result.items);taskReady.value=true;taskTotal.value=result.total;taskPages.value=result.totalPages;
  if(result.totalPages>0&&page>result.totalPages)clampedPage=result.totalPages;
 }catch(e){if(current())throw e;}finally{if(sequence===taskPageSequence){taskPageLoading.value=false;taskRequesting=false;}}
 if(clampedPage!==null&&current()){taskPage.value=clampedPage;await fetchTaskPage(true);}
}
async function searchDatasets(){datasetQuery.value=datasetSearch.value.trim();datasetPage.value=1;await guarded(()=>fetchDatasetPage(true));}
async function changeDatasetPage(delta){datasetPage.value=Math.max(1,datasetPage.value+delta);await guarded(()=>fetchDatasetPage(true));}
async function changeTaskPage(delta){taskPage.value=Math.max(1,taskPage.value+delta);await guarded(()=>fetchTaskPage(true));}
async function filterTasks(){taskPage.value=1;await guarded(()=>fetchTaskPage(true));}
async function refreshTasks(){await guarded(()=>Promise.all([fetchTaskPage(true),refreshDetail(true)]));}
async function loadSelectedDataset(id){
 if(dataset.value?.id===id){selected.value=id;return;}
 const sequence=++datasetSequence,previous=dataset.value;selected.value=id;datasetLoading.value=!!id;
 try{const data=id?await api('/datasets/'+id):null;if(disposed||sequence!==datasetSequence||selected.value!==id)return;
  dataset.value=data;rulePlan.value=null;fields.value=data?[...data.columns]:[];keys.value=data?[data.columns[0]]:[];
 }catch(e){if(sequence===datasetSequence){selected.value=previous?.id||'';throw e;}}finally{if(sequence===datasetSequence)datasetLoading.value=false;}
}
async function selectDataset(){await guarded(()=>loadSelectedDataset(selected.value));}
async function loadReport(id,sequence){
 if(detail.value?.status!=='SUCCEEDED'){report.value=null;return;}
 if(report.value?.task?.id===id)return;
 const result=await api(`/tasks/${id}/report`);if(!disposed&&activeTaskId.value===id&&detailSequence===sequence)report.value=result;
}
async function refreshDetail(force=false){
 const id=activeTaskId.value;if(!id||(detailLoading.value&&!force))return;
 const sequence=++detailSequence;detailLoading.value=true;
 const current=()=>!disposed&&sequence===detailSequence&&activeTaskId.value===id;
 try{const result=await api('/tasks/'+id);if(!current())return;detail.value=stableValue(detail.value,result);await loadReport(id,sequence);}
 catch(e){if(current())throw e;}finally{if(sequence===detailSequence)detailLoading.value=false;}
}
async function refresh(){await settleRequests([fetchDatasetPage(),fetchTaskPage(),refreshDetail(),refreshExecutionService(),refreshOverview()]);}
async function setActiveTask(id){if(activeTaskId.value===id&&detail.value){await refreshDetail(true);return;}activeTaskId.value=id;detail.value=null;samples.value=null;report.value=null;++previewSequence;await refreshDetail(true);}
async function show(id){navigate('tasks');await guarded(()=>setActiveTask(id));}
async function createdTask(task,message){taskStatus.value='ALL';taskPage.value=1;await Promise.all([setActiveTask(task.id),fetchTaskPage(true),fetchDatasetPage(true)]);notice.value=message;navigate('tasks');}
async function upload(event){const file=event.target.files[0];if(!file)return;busy.value=true;await guarded(async()=>{const form=new FormData();form.append('file',file);const data=await api('/datasets',{method:'POST',body:form});await loadSelectedDataset(data.id);await fetchDatasetPage(true);notice.value='上传成功，原始文件已保留';});busy.value=false;event.target.value='';}
async function uploadParts(event){const files=Array.from(event.target.files||[]);if(!files.length)return;busy.value=true;await guarded(async()=>{if(files.length<2||files.length>16)throw new Error('请选择2..16份同结构CSV');const form=new FormData();for(const file of files)form.append('files',file);const data=await api('/datasets/csv-parts',{method:'POST',body:form});await loadSelectedDataset(data.id);await fetchDatasetPage(true);notice.value='分片已合并注册，各原文件和来源指纹已保留';});busy.value=false;event.target.value='';}
async function submit(){busy.value=true;await guarded(async()=>{const task=await api('/tasks',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({datasetId:selected.value,...(executionFormat.value==='CSV_SINGLE'?{}:{executionInput:{format:executionFormat.value,partitions:4}}),...(rulePlan.value?.templateId?{templateId:rulePlan.value.templateId,templateRevision:rulePlan.value.templateRevision}:rulePlan.value?{rules:rulePlan.value.rules}:{fields:fields.value,keys:keys.value})})});await createdTask(task,'检测任务已提交');});busy.value=false;}
async function rerun(useSelected=false){busy.value=true;await guarded(async()=>{const task=await api(`/tasks/${detail.value.id}/rerun`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(useSelected?{datasetId:selected.value}:{})});await createdTask(task,'复检已提交，沿用原任务规则并保留历史结果');});busy.value=false;}
async function govern(policy){busy.value=true;await guarded(async()=>{const task=await api(`/tasks/${detail.value.id}/govern`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(policy)});await createdTask(task,'治理已提交：生成独立版本后自动复检，原始数据保留');});busy.value=false;}
async function cancelTask(){
 const id=detail.value?.id;if(!id||!['QUEUED','RUNNING'].includes(detail.value.status))return;
 busy.value=true;
 await guarded(async()=>{const result=await api(`/tasks/${id}/cancel`,{method:'POST'});notice.value=result.status==='CANCELLED'?'任务已取消，原数据和历史版本保留':'取消请求已提交，任务状态会自动更新';});
 await guarded(()=>Promise.all([fetchTaskPage(true),refreshDetail(true)]),false);busy.value=false;
}
async function useVersion(id){navigate('datasets');await guarded(()=>loadSelectedDataset(id));}
async function preview(rule){const id=activeTaskId.value,sequence=++previewSequence;await guarded(async()=>{const rows=await api(`/tasks/${id}/rules/${rule.id}/preview`);if(!disposed&&id===activeTaskId.value&&sequence===previewSequence)samples.value={rule,rows};});}
const views=[{id:'overview',name:'工作台概览',icon:'◫',description:'从真实数据出发，跟踪质量检测与治理进展。'},{id:'datasets',name:'数据集管理',icon:'▤',description:'统一接入、预览与追溯，原始文件始终保留。'},{id:'rules',name:'检测规则',icon:'☷',description:'选择数据集，配置规则或沿用已保存的模板修订。'},{id:'tasks',name:'检测任务与报告',icon:'◷',description:'跟踪执行状态，阅读完整指标与独立质量报告。'},{id:'versions',name:'数据治理与版本',icon:'◇',description:'从检测结果生成独立版本，保留父子关系与治理审计。'},{id:'system',name:'系统运行状态',icon:'⊙',description:'查看计算方式、服务可用性与自动更新策略。'}];
const sourceView=ref('csv');
const view=ref('overview'),currentView=computed(()=>views.find(v=>v.id===view.value)),positions=new Map();
async function navigate(id){if(id===view.value)return;positions.set(view.value,window.scrollY);view.value=id;await nextTick();window.scrollTo({top:positions.get(id)||0,behavior:'instant'});}
const overview=ref(null);
async function refreshOverview(){const [data,recent]=await Promise.all([api('/datasets/page?page=1&pageSize=1&q='),api('/tasks/page?page=1&pageSize=5&status=ALL')]);if(!disposed)overview.value=stableValue(overview.value,{datasets:data.total,tasks:recent.total,recent:recent.items});}
const polling=pollingLoop(()=>guarded(refresh,false),()=>pollDelay(tasks.value.some(isActive)||overview.value?.recent.some(isActive)||isActive(detail.value),document.hidden));
watch(()=>isActive(detail.value),()=>polling.reschedule());
onMounted(()=>polling.start());onUnmounted(()=>{disposed=true;polling.stop();});
</script>

<template>
 <div class="app-shell">
 <a class="skip-link" href="#workspace">跳到主内容</a>
 <aside class="sidebar"><a class="brand" href="#" @click.prevent="navigate('overview')"><span class="brand-mark">S</span>SparkDQ</a><div class="workspace-label">数据质量管理平台</div>
 <nav aria-label="工作区导航"><button v-for="item in views" :key="item.id" :class="{selected:view===item.id}" :aria-current="view===item.id?'page':undefined" @click="navigate(item.id)"><span aria-hidden="true">{{item.icon}}</span>{{item.name}}</button></nav>
 <div class="sidebar-foot"><span class="status SUCCEEDED">原始数据只读保留</span><p>检测 · 治理 · 独立复检</p><small>SparkDQ / DATA QUALITY</small></div></aside>
 <div class="workspace"><header class="topbar"><span>工作区 <span class="breadcrumb">/ {{currentView.name}}</span></span><span class="environment-badge" :class="{offline:pollError}"><i></i>{{pollError?'连接待恢复':executionServiceView.label}}</span></header>
 <main id="workspace" tabindex="-1"><div class="page-heading"><div><p class="eyebrow">DATA QUALITY WORKSPACE</p><h1>{{currentView.name}}</h1><p>{{currentView.description}}</p></div><button v-if="view==='overview'" class="primary compact" @click="navigate('datasets')">接入数据 <span aria-hidden="true">＋</span></button></div>
 <div class="feedback-region" aria-live="polite">
  <div v-if="error" class="alert" role="alert">{{error}}<button class="text-button dismiss" aria-label="关闭操作错误" @click="error=''">×</button></div><div v-if="pollError" class="alert" role="alert">页面状态暂时无法更新：{{pollError}}</div><div v-if="notice" class="notice" role="status">{{notice}}<button class="text-button dismiss" aria-label="关闭通知" @click="notice=''">×</button></div>
 </div>
 <section v-show="view==='overview'" class="overview" aria-label="工作台概览">
 <div class="metric-grid"><article class="metric"><span>已注册数据集</span><strong>{{overview?.datasets??'—'}}</strong><small>包含原始快照与治理版本</small></article><article class="metric"><span>检测与治理任务</span><strong>{{overview?.tasks??'—'}}</strong><small>所有状态的历史任务总数</small></article><article class="metric"><span>最近任务状态</span><strong class="metric-state">{{overview?.recent?.length?labels[overview.recent[0].status]:'暂无任务'}}</strong><small>以最近创建的一条任务为准</small></article></div>
 <div class="overview-grid"><section class="card"><div class="section-title"><h2>最近执行记录</h2><button class="text-button" @click="navigate('tasks')">查看全部 →</button></div><div v-if="!overview" class="empty">正在读取工作台…</div><p v-else-if="!overview.recent.length" class="empty">接入数据后即可创建第一个检测任务。</p><button v-for="t in overview?.recent||[]" :key="t.id" class="task recent-task" @click="show(t.id)"><div><strong>{{t.dataset_name}}</strong><small>{{t.governance_id?'治理与复检':'质量检测'}} · {{t.id.slice(0,8)}}</small></div><span class="status" :class="t.status">{{labels[t.status]}}</span></button></section>
 <section class="card workflow-card"><div class="section-title"><h2>完整的数据质量闭环</h2><span class="subtle-label">工作流程</span></div><ol class="workflow"><li><b>01</b><div><strong>接入与冻结快照</strong><p>统一管理 CSV、Parquet 和业务数据源。</p></div></li><li><b>02</b><div><strong>按规则检测</strong><p>定位缺失、重复、IQR 异常和 FD 冲突。</p></div></li><li><b>03</b><div><strong>治理并独立复检</strong><p>生成新版本，对比真实指标，原始数据不覆盖。</p></div></li></ol><button class="secondary" @click="navigate('datasets')">从数据集开始 →</button></section></div>
 </section>
 <section v-show="view==='system'" class="card system-card"><h2>计算与刷新</h2><div class="service-execution" aria-label="运行方式"><strong>{{executionServiceView.label}}</strong><p v-if="executionServiceView.blocked" role="status">部分任务仍在核对资源，暂时不能提交新检测。状态确认后自动恢复。</p><p v-else>检测结果、治理版本和历史记录持续保留。</p></div><p>活跃任务约每3秒更新；静置时约每15秒检查；后台标签页约每30秒检查。请求完成后再安排下次检查。</p><p class="hint">连接失败时保留上次成功数据，并自动重试。各导入任务独立跟踪，检测、取消和治理状态持续更新。</p></section>
 <div v-show="view==='datasets'" class="source-area"><div class="section-title"><h2>数据接入</h2><span class="subtle-label">快照入库，来源可追溯</span></div><div class="source-tabs" role="group" aria-label="数据源入口"><button v-for="item in [{id:'csv',name:'CSV 文件'},{id:'parquet',name:'原生 Parquet'},{id:'mysql',name:'MySQL 业务快照'},{id:'preset',name:'受控预置数据'}]" :key="item.id" :class="{selected:sourceView===item.id}" :aria-pressed="sourceView===item.id" @click="sourceView=item.id">{{item.name}}</button></div><div class="source-grid"><p v-show="sourceView==='csv'" class="hint">上传单份 CSV 或注册多份同结构分片。原始文件独立保留，注册后在下方选择数据集。</p>  <ControlledFilesPanel v-show="sourceView==='preset'" :api="api" @dataset="id=>guarded(async()=>{await loadSelectedDataset(id);await fetchDatasetPage(true);})"/>
  <MysqlImportPanel v-show="sourceView==='mysql'" :api="api" @dataset="id=>guarded(async()=>{await loadSelectedDataset(id);await fetchDatasetPage(true);})"/>
  <ImportPanel v-show="sourceView==='parquet'" :api="api" @dataset="id=>guarded(async()=>{await loadSelectedDataset(id);await fetchDatasetPage(true);})"/>
</div></div>
  <div class="layout" v-show="['datasets','rules','tasks'].includes(view)">
   <section class="card dataset-card" v-show="view==='datasets'||view==='rules'"><div class="section-title"><div><h2>{{view==='rules'?'规则工作区':'数据集目录'}}</h2><p class="hint">{{view==='rules'?'配置遵循已保存的检测语义与默认值':'搜索、选择并查看数据快照'}}</p></div><div class="button-group" v-show="view==='datasets'"><label class="upload secondary">注册 CSV 分片<input aria-label="注册 CSV 分片" type="file" accept=".csv" multiple :disabled="busy" @change="uploadParts"></label><label class="upload">上传 CSV<input aria-label="上传 CSV" type="file" accept=".csv" :disabled="busy" @change="upload"></label></div></div>
    <details v-show="view==='datasets'" class="input-limits"><summary>CSV 格式与分片注册说明 · 单次最大 20 MiB</summary>    <p class="hint">分片注册：2..16份同字段/顺序CSV；原文件总量及合并文件均最多20MiB，按提交顺序合并，保留各原文件。重复记录照常保留，检测后再决定治理。</p>
    <p class="hint">UTF-8 · 最大 20 MiB · 表头为英文字母开头的字母、数字或下划线</p>
</details>
    <form class="search-row" @submit.prevent="searchDatasets"><label for="dataset-search">按名称查找数据集<input id="dataset-search" v-model="datasetSearch" type="search" maxlength="200" placeholder="输入名称关键词后搜索"></label><button class="secondary" type="submit">搜索</button></form>
    <label class="form-label" for="dataset">已注册数据集</label><select id="dataset" v-model="selected" :disabled="busy||datasetLoading" @change="selectDataset"><option value="">请选择数据集</option><option v-if="selectedOutsidePage" :value="selected">已选 · {{dataset?.id===selected?dataset.name:selected.slice(0,8)}}{{dataset?.id===selected?' · '+dataset.row_count+' 行':''}}（不在本页）</option><option v-for="d in datasets" :key="d.id" :value="d.id">{{d.name}} · {{d.row_count}} 行</option></select>
    <div v-show="view==='datasets'" class="dataset-catalog table-wrap"><table aria-label="数据集目录"><thead><tr><th>数据集名称</th><th>记录数</th><th>版本编号</th></tr></thead><tbody><tr v-for="d in datasets" :key="d.id" :class="{'selected-row':selected===d.id}"><td><button class="text-button" :aria-pressed="selected===d.id" @click="guarded(()=>loadSelectedDataset(d.id))">{{d.name}}</button></td><td>{{d.row_count}}</td><td>{{d.id.slice(0,8)}}</td></tr></tbody></table></div>
    <div class="pagination" aria-label="数据集分页"><button class="text-button" :disabled="datasetPage<=1||datasetPageLoading" @click="changeDatasetPage(-1)">上一页数据</button><span>第 {{datasetPage}} / {{Math.max(1,datasetPages)}} 页 · {{datasetTotal}} 个{{datasetQuery?'匹配项':'数据集'}}</span><button class="text-button" :disabled="!datasetPages||datasetPage>=datasetPages||datasetPageLoading" @click="changeDatasetPage(1)">下一页数据</button></div>
    <p v-if="datasetPageLoading||datasetLoading" class="hint" role="status">正在加载数据集…</p>
    <p v-if="dataset" class="hint current-selection">当前选择：{{dataset.name}} · {{dataset.row_count}} 行<span v-if="selectedOutsidePage">；翻页或搜索会保留此选择</span></p>
    <p v-if="!datasetPageLoading&&!datasets.length" class="hint">{{datasetQuery?'没有匹配的数据集，请调整关键词。':'当前页没有数据集。'}}</p>
    <template v-if="dataset">
     <div v-show="view==='datasets'" class="dataset-metadata"><div class="selection-summary"><div><strong>{{dataset.name}}</strong><p>{{dataset.row_count}} 行 · {{dataset.columns.length}} 个字段</p></div><button class="secondary" @click="navigate('rules')">配置检测规则 →</button></div><DatasetSource :dataset="dataset"/>
     <a :href="`/api/datasets/${dataset.id}/download`" download>下载此版本 CSV</a>
     </div><div v-show="view==='rules'" class="rule-workspace"><fieldset><legend>空值检测字段</legend><label v-for="c in dataset.columns" :key="c" class="check"><input type="checkbox" :value="c" v-model="fields" :disabled="!!rulePlan?.templateId">{{c}}</label></fieldset>
     <fieldset><legend>重复判定业务键（可多选）</legend><label v-for="c in dataset.columns" :key="c" class="check"><input type="checkbox" :value="c" v-model="keys" :disabled="!!rulePlan?.templateId">{{c}}</label></fieldset>
     <p class="hint">空值检测包含缺失值、空字符串和纯空格。业务键按原始值精确比较，空键参与分组。</p>
     <RuleManager :key="dataset.id" :columns="dataset.columns" :base-rules="baseRules" :api="api" v-model:plan="rulePlan" @busy="managerBusy=$event" @error="error=$event"/>
     <label class="form-label" for="execution-format">检测输入方式</label>
     <select id="execution-format" v-model="executionFormat" :disabled="busy"><option value="CSV_SINGLE">原始 CSV（默认）</option><option value="CSV">分片 CSV 执行副本</option><option value="PARQUET">Parquet 执行副本</option></select>
     <p class="hint">执行副本会先转换并核对数据，再开始检测；原 CSV 与下载保持保留。用于格式验证，当前小样本未显示提速。复检和治理沿用原任务的输入方式。</p>
     <button class="primary" :disabled="busy||datasetLoading||!canStartTasks||managerBusy||rulePlan?.valid===false||(!rulePlan?.templateId&&(!fields.length||!keys.length))" @click="submit">开始质量检测 →</button></div>
     <details open v-show="view==='datasets'"><summary>字段与原始数据预览（最多 10 行）</summary><p class="hint">预览每单元格最多256字符，长文本以省略号结尾；完整内容请下载此版本CSV。</p><div class="table-wrap"><table><thead><tr><th v-for="c in dataset.columns" :key="c">{{c}}</th></tr></thead><tbody><tr v-for="(row,i) in dataset.preview" :key="i"><td v-for="(v,j) in row" :key="j">{{v===''?'（空）':v}}</td></tr></tbody></table></div></details>
    </template><p v-else class="empty">上传测试 CSV 或选择已有数据集开始。</p>
   </section>

  </div>
  <div v-show="view==='versions'"><section class="card version-intro"><h2>每次治理都是一个独立版本</h2><p>原始数据 → 检测结果 → 治理策略 → 新数据版本 → 独立复检</p><p class="hint">原始文件始终保留。选择数据集查看父子版本；打开已完成任务配置治理，查看审计和前后指标。</p><div class="button-group"><button class="secondary" @click="navigate('datasets')">选择数据集</button><button class="secondary" @click="navigate('tasks')">打开任务与治理</button></div><p v-if="!dataset" class="empty">尚未选择数据集。</p></section><DatasetHistory v-if="dataset" :key="dataset.id" :dataset="dataset" :api="api" @task="show" @dataset="useVersion"/></div>
 <div class="tasks-workspace" v-show="view==='tasks'">   <section class="card task-card" v-show="view==='tasks'"><div class="section-title"><h2>检测任务</h2><button class="text-button" @click="refreshTasks">刷新任务</button></div>
    <label class="form-label" for="task-status">按状态筛选</label><select id="task-status" v-model="taskStatus" @change="filterTasks"><option value="ALL">全部状态</option><option v-for="(label,status) in labels" :key="status" :value="status">{{label}}</option></select>
    <p class="hint">自动更新 · {{taskTotal}} 个{{taskStatus==='ALL'?'任务':'匹配任务'}} · 每页 {{PAGE_SIZE}} 项<span v-if="taskPageLoading" class="inline-loading"> · 正在读取…</span></p>
    <div class="pagination" aria-label="任务分页"><button class="text-button" :disabled="taskPage<=1||taskPageLoading" @click="changeTaskPage(-1)">上一页任务</button><span>第 {{taskPage}} / {{Math.max(1,taskPages)}} 页</span><button class="text-button" :disabled="!taskPages||taskPage>=taskPages||taskPageLoading" @click="changeTaskPage(1)">下一页任务</button></div>
    <div class="task-list"><div v-if="!taskPageLoading&&!tasks.length" class="empty">{{taskStatus==='ALL'?'当前页没有检测任务':'当前页没有此状态的任务'}}</div>
    <button v-for="t in tasks" :key="t.id" class="task" :class="{active:detail?.id===t.id}" @click="show(t.id)"><div><strong>{{t.governance_id?'治理 · ':''}}{{t.dataset_name}}</strong><small>{{t.id.slice(0,8)}} · {{t.duration_ms==null?'等待执行':(t.duration_ms/1000).toFixed(2)+' 秒'}}</small></div><span class="status" :class="t.status">{{labels[t.status]}}</span></button></div>
   </section><div class="task-detail-column"><section v-if="!detail&&!detailLoading" class="card detail-placeholder"><span aria-hidden="true">◷</span><h2>选择一个检测任务</h2><p>查看执行状态、规则指标、异常预览与质量报告。</p><p class="hint">已完成任务还可创建治理版本或独立复检。</p></section>  <p v-if="view==='tasks'&&detailLoading&&!detail" class="empty" role="status">正在加载任务详情…</p>
  <section class="card result" v-if="detail" v-show="view==='tasks'"><div class="section-title"><h2>任务详情与质量报告</h2><span class="status" :class="detail.status">{{labels[detail.status]}}</span></div><p class="hint">任务 {{detail.id}}</p><p v-if="detail.template" class="hint">规则模板 {{detail.template.name}} · 修订 {{detail.template.revision}}</p>
   <ExecutionPanel :task="detail"/>
   <div v-if="['QUEUED','RUNNING'].includes(detail.status)" class="cancel-panel"><p class="hint">取消只停止当前任务，原数据和历史版本保留。</p><button class="secondary danger" :disabled="busy" @click="cancelTask">{{busy?'正在处理…':'取消此任务'}}</button></div>
   <p v-if="detail.status==='CANCELLING'" class="cancellation-note" role="status">正在停止计算并核对资源状态。两项确认后将显示“已取消”。</p>
   <p v-if="detail.status==='CANCELLED'" class="cancellation-note" role="status">此任务已取消，原数据和历史版本保留。可按原规则创建新的复检任务。</p>
   <ReportActions :task="detail"/>
   <div v-if="['SUCCEEDED','FAILED','CANCELLED'].includes(detail.status)">
    <button class="text-button" :disabled="busy||!canStartTasks" @click="rerun()">{{detail.governance?.output_dataset_id?'按原规则复检治理版本':'按原规则复检原数据'}}</button>
    <button class="text-button" :disabled="busy||datasetLoading||!canStartTasks||!selected||selected===detail.dataset_id" @click="rerun(true)">按原规则复检当前所选数据</button>
    <p class="hint">复检会创建独立任务，使用本任务保存的规则；规则工作区字段勾选不影响复检。可上传治理后的 CSV 并选择它进行复检。</p>
   </div>
   <details class="execution-metadata"><summary>执行输入、来源任务与完整指纹</summary>   <p v-if="detail.snapshot" class="hint">执行输入：{{inputLabels[detail.snapshot.executionInput?.format||'CSV_SINGLE']}}<br>统计口径 {{detail.snapshot.semantics_version}} · 输入 SHA-256 {{detail.snapshot.input_sha256}}<br>
    <button v-if="detail.snapshot.parent_task_id" class="text-button" @click="show(detail.snapshot.parent_task_id)">查看来源任务 {{detail.snapshot.parent_task_id}}</button>
   </p>
   <p v-else class="hint">历史任务未记录输入快照；复检将沿用已保存的规则并为新任务建立快照。</p>
</details>
   <div v-if="detail.governance" class="notice">
    <p>治理策略：{{policyText(detail.governance.policy)}}。</p>
    <p class="hint" v-if="detail.governance.policy.policyVersion===2">执行顺序：标准化 → 填充 → 移除仍缺失记录 → 去重。配置已保存在本任务中。</p>
    <template v-if="detail.governance.output_dataset_id">
     <p>输入 {{detail.governance.input_rows}} 行 → 输出 {{detail.governance.output_rows}} 行，移除 {{detail.governance.removed_rows}} 行。下方指标是新版本文件的复检结果。</p>
     <div v-if="detail.governance.audit?.transformations?.length" class="table-wrap"><table><thead><tr><th>字段</th><th>操作</th><th>实际改变的行数</th><th>匹配行数</th></tr></thead><tbody><tr v-for="(change,i) in detail.governance.audit.transformations" :key="i"><td>{{change.column}}</td><td>{{operationLabels[change.operation]}}<details v-if="change.entries"><summary>{{change.entries.length}} 条映射明细</summary><div v-for="(entry,j) in change.entries" :key="j" style="white-space:pre-wrap">{{literalText(entry.from)}} → {{literalText(entry.to)}}；匹配 {{entry.matchedRows}} 行，改变 {{entry.changedRows}} 行</div></details></td><td>{{change.changedRows}}</td><td>{{change.matchedRows??'—'}}</td></tr></tbody></table><p class="hint">按每一步当时的数据统计；不同操作可能改变同一记录，不能相加为不同记录数。</p></div>
     <div v-if="detail.governance.audit?.deduplication" class="hint"><p>已应用保留优先级：{{retentionText(detail.governance.audit.deduplication.order)}}。</p><p>去重前 {{detail.governance.audit.deduplication.inputRows}} 行，重复业务键 {{detail.governance.audit.deduplication.duplicateGroups}} 组；优先级并列时按全字段字典序保留，NULL 在前。</p></div>
     <p v-if="detail.governance.audit?.droppedMissingRows!==undefined" class="hint">移除仍缺失记录 {{detail.governance.audit.droppedMissingRows}} 行；去重移除 {{detail.governance.audit.deduplicatedRows}} 行。</p>
     <button class="text-button" @click="useVersion(detail.governance.output_dataset_id)">选择生成的数据版本</button>
     <a :href="`/api/datasets/${detail.governance.output_dataset_id}/download`" download>下载治理版本 CSV</a>
    </template>
   </div>
   <details v-if="detail.error"><summary>{{detail.status==='CANCELLED'?'查看取消原因':'查看任务处理说明'}}</summary><pre>{{detail.error}}</pre></details>
   <p v-if="detail.status==='QUEUED'||detail.status==='RUNNING'" class="empty">Spark 正在准备或计算，请稍候…</p>
   <template v-if="detail.status==='SUCCEEDED'">
    <GovernancePanel :key="detail.id" :task="detail" :api="api" :busy="busy||!canStartTasks" @submit="govern" @error="error=$event"/>

    <div v-if="report?.comparison?.length">
     <h3>前后指标对比</h3><p class="hint">来源 {{report.sourceTaskId}} · {{report.beforeRows}} 行 → {{report.afterRows}} 行。各规则异常可能重叠；移除异常记录不等于修复原始值。</p>
     <div class="table-wrap"><table><thead><tr><th>规则 / 字段</th><th>之前异常行数</th><th>之后异常行数</th><th>减少</th></tr></thead><tbody><tr v-for="r in report.comparison" :key="r.type+JSON.stringify(r.columns)+JSON.stringify(r.params)"><td>{{typeLabels[r.type]}} · {{r.columns.join(' + ')}}<small v-if="Object.keys(r.params||{}).length">{{configText(r)}}</small></td><td>{{r.before}}</td><td>{{r.after}}</td><td>{{r.reducedBy}}</td></tr></tbody></table></div>
    </div>
    <IqrComparison :rows="report?.comparison||[]"/>
    <p v-if="report?.comparisonUnavailable" class="hint">{{report.comparisonUnavailable}}</p>
    <div class="stats"><div><strong>{{detail.total_rows}}</strong><span>检测记录</span></div><div><strong>{{(detail.duration_ms/1000).toFixed(2)}} s</strong><span>执行耗时</span></div><div><strong>{{detail.spark_version}}</strong><span>Spark 引擎版本</span></div></div>
    <h3>各规则异常行数</h3><p class="hint">各规则可能命中同一条记录，不能相加为异常记录总数。FD 展示冲突组的全部参与行。</p><AnomalyChart :rules="detail.rules" :labels="typeLabels"/>
    <div class="table-wrap"><table><thead><tr><th>规则 / 字段</th><th>统计结果</th><th>异常明细</th></tr></thead><tbody><tr v-for="r in detail.rules" :key="r.id"><td>{{typeLabels[r.type]}} · {{r.columns.join(' + ')}}<small v-if="Object.keys(r.params||{}).length">{{configText(r)}}</small><small>规则 {{r.id.slice(0,8)}} / 结果 {{r.result_id?.slice(0,8)}}</small></td><td v-if="r.type==='MISSING'">缺失 {{r.metrics.nullCount}} · 空字符串 {{r.metrics.emptyCount}} · 纯空格 {{r.metrics.whitespaceCount}} · 合计 {{r.metrics.blankCount}}</td><td v-else-if="r.type==='DUPLICATE'">{{r.metrics.duplicateGroups}} 组 · 涉及 {{r.metrics.duplicateRows}} 行 · 多余 {{r.metrics.excessRows}} 行</td><td v-else-if="r.type==='FD'">有效 {{r.metrics.checkedCount}} · 跳过 {{r.metrics.skippedCount}} · 检查 {{r.metrics.checkedGroups}} 组 · 冲突 {{r.metrics.conflictGroups}} 组 / {{r.metrics.conflictRows}} 行<small>含NULL {{r.metrics.skippedNullCount}} / 空串 {{r.metrics.skippedEmptyCount}} / 纯空格 {{r.metrics.skippedWhitespaceCount}}（原因可能重叠）</small><small>{{r.metrics.status}} · {{r.metrics.algorithmVersion}}；冲突参与不等于该行必为错误来源。</small></td><td v-else-if="r.type==='IQR'">状态 {{r.metrics.status}} · 异常 {{r.metrics.invalidCount}} · 有效 {{r.metrics.checkedCount}} · 跳过 {{r.metrics.skippedCount}}（空值 {{r.metrics.skippedBlankCount}} / 非数值 {{r.metrics.nonNumericCount}} / 超精度域 {{r.metrics.outOfDomainCount}}）<small>Q1 {{r.metrics.q1??'未计算'}} · Q3 {{r.metrics.q3??'未计算'}} · IQR {{r.metrics.iqr??'未计算'}} · 下界 {{r.metrics.lowerBound??'未计算'}} · 上界 {{r.metrics.upperBound??'未计算'}}</small><small>严格区间外为异常；不足4个有效值不作正常结论。{{r.metrics.algorithmVersion}}</small></td><td v-else>异常 {{r.metrics.invalidCount}} · 已检查 {{r.metrics.checkedCount}} · 跳过空值 {{r.metrics.skippedBlankCount}}<span v-if="r.metrics.nonNumericCount!==undefined"> · 非数值 {{r.metrics.nonNumericCount}}</span></td><td><button class="text-button" @click="preview(r)">预览异常</button></td></tr></tbody></table></div>
    <p v-if="samples" class="hint">异常预览最多50行，每单元格最多256字符；完整异常文件保持原值与多重性。</p><div v-if="samples" class="samples"><h3>异常数据 · {{samples.rule.columns.join(' + ')}} <small>最多 50 行</small></h3><p v-if="!samples.rows.length" class="empty">此规则没有异常记录</p><div v-else class="table-wrap"><table><thead><tr><th v-for="c in Object.keys(samples.rows[0])" :key="c">{{c}}</th></tr></thead><tbody><tr v-for="(row,i) in samples.rows" :key="i"><td v-for="(v,c) in row" :key="c">{{v===null?'NULL':v===''?'（空字符串）':v}}</td></tr></tbody></table></div><details><summary>存储路径与完整 ID</summary><pre>{{samples.rule}}</pre></details></div>
   </template>
  </section>
</div></div>
  <footer>SparkDQ · 原始文件只读保留 · 治理生成独立版本</footer>
 </main></div></div>
</template>
