<script setup>
import IqrComparison from './IqrComparison.vue';
import {ref,computed,onMounted,onUnmounted} from 'vue';
import {stableValue,isActive,pollDelay,pollingLoop} from './refresh-state.mjs';
import {requestOwner,taskLabels,ruleLabels,historyRole,comparisonState} from './dataset-history.mjs';
const props=defineProps({dataset:{type:Object,required:true},api:{type:Function,required:true}});
const emit=defineEmits(['task','dataset']);
const page=ref(1),status=ref('ALL'),items=ref([]),total=ref(0),pages=ref(0),loading=ref(false),error=ref('');
const selectedReport=ref(null),reportLoading=ref(false),reportError=ref(''),reportTask=ref('');
const owner=requestOwner(),reports=requestOwner(),change=computed(()=>comparisonState(selectedReport.value));
const lineage=ref(props.dataset);
let requesting=false,ready=false;
async function refresh(force=false){
 if(requesting&&!force)return;
 const current=owner.begin(),identity=props.dataset.id,requestedPage=page.value;requesting=true;loading.value=!ready||force;
 try{
  const [result,metadata]=await Promise.all([props.api(`/datasets/${identity}/tasks?`+new URLSearchParams({page:requestedPage,pageSize:5,status:status.value})),props.api(`/datasets/${identity}`)]);if(!current())return;lineage.value=stableValue(lineage.value,metadata);
  if(result.totalPages&&requestedPage>result.totalPages){page.value=result.totalPages;loading.value=false;requesting=false;await refresh(true);return;}
  items.value=stableValue(items.value,result.items);ready=true;total.value=result.total;pages.value=result.totalPages;error.value='';
 }catch(e){if(current())error.value=e.message;}finally{if(current()){loading.value=false;requesting=false;}}
}
async function filter(){page.value=1;reports.invalidate();selectedReport.value=null;reportTask.value='';reportLoading.value=false;reportError.value='';await refresh(true);}
async function turn(delta){page.value+=delta;reports.invalidate();selectedReport.value=null;reportTask.value='';reportLoading.value=false;reportError.value='';await refresh(true);}
async function compare(task){
 const current=reports.begin();reportTask.value=task.id;selectedReport.value=null;reportError.value='';reportLoading.value=true;
 try{const report=await props.api(`/tasks/${task.id}/report`);if(current())selectedReport.value=report;}
 catch(e){if(current())reportError.value=e.message;}finally{if(current())reportLoading.value=false;}
}
function config(rule){const p=rule.params||{};if(rule.type==='TYPE')return p.dataType;if(rule.type==='RANGE')return `${p.min??'不限'} ～ ${p.max??'不限'}（含边界）`;if(rule.type==='FD')return rule.columns.join('+')+' → '+(p.dependentColumns||[]).join('+');if(rule.type==='IQR')return 'k='+p.k+'，每次重新拟合';if(rule.type==='REGEX')return p.pattern;return '';}
const polling=pollingLoop(()=>refresh(),()=>pollDelay(items.value.some(isActive),document.hidden));
onMounted(()=>polling.start());
onUnmounted(()=>{polling.stop();owner.close();reports.close();});
</script>
<template>
 <section class="dataset-history card" aria-label="数据集历史">
  <div class="section-title"><h3>此数据集的任务与版本</h3><button type="button" class="text-button" @click="refresh(true)">刷新数据集历史</button></div>
  <p><strong>{{dataset.name}}</strong> · {{dataset.row_count}} 行</p>
  <p class="hint">仅包含以此版本为输入的检测/治理，以及生成此版本的治理任务；不混入其他数据集。活跃时约3秒、静置时约15秒自动更新。</p>
  <div v-if="lineage.version" class="history-origin"><strong>治理版本来源</strong><button type="button" class="text-button" @click="emit('dataset',lineage.version.parent_dataset_id)">查看父版本</button><button type="button" class="text-button" @click="emit('task',lineage.version.governance_task_id)">查看生成任务</button></div>
  <div v-if="lineage.derivedVersions?.length" class="history-children"><strong>已生成的版本</strong><ul><li v-for="version in lineage.derivedVersions" :key="version.dataset_id"><button type="button" class="text-button" @click="emit('dataset',version.dataset_id)">{{version.row_count}} 行 · {{version.dataset_id.slice(0,8)}}</button><button type="button" class="text-button" @click="emit('task',version.governance_task_id)">查看治理任务</button></li></ul></div>
  <label :for="`dataset-history-status-${dataset.id}`" class="form-label">筛选此数据集的任务状态</label>
  <select :id="`dataset-history-status-${dataset.id}`" v-model="status" @change="filter"><option value="ALL">全部状态</option><option v-for="(name,key) in taskLabels" :key="key" :value="key">{{name}}</option></select>
  <p v-if="error" class="alert" role="alert">历史记录加载失败：{{error}} <button type="button" class="text-button" @click="refresh(true)">重试历史记录</button></p>
  <p v-if="loading" class="hint" role="status">正在更新此数据集历史…</p>
  <p v-if="!loading&&!error&&!items.length" class="hint">{{status==='ALL'?'此版本还没有关联任务。':'此状态下没有关联任务。'}}</p>
  <div v-if="items.length" class="history-list">
   <article v-for="task in items" :key="task.id" class="history-task">
    <div class="section-title"><strong>{{historyRole(task,dataset.id)}}</strong><span :class="['status',task.status]">{{taskLabels[task.status]}}</span></div>
    <p class="hint">{{task.created_at}} · {{task.id.slice(0,8)}}<br v-if="task.status==='SUCCEEDED'"><span v-if="task.status==='SUCCEEDED'">结果 {{task.total_rows}} 行{{task.output_dataset_id?'（生成的治理版本）':''}}</span></p>
    <div class="history-buttons"><button type="button" class="text-button" @click="emit('task',task.id)">查看任务详情</button><button v-if="task.status==='SUCCEEDED'" type="button" class="text-button" @click="compare(task)">查看质量变化</button><a v-if="['SUCCEEDED','FAILED','CANCELLED'].includes(task.status)" :href="`/api/tasks/${task.id}/report.html`" target="_blank" rel="noopener">{{task.status==='SUCCEEDED'?'阅读报告':'处理记录'}}</a></div>
    <button v-if="task.output_dataset_id&&task.output_dataset_id!==dataset.id" type="button" class="text-button" @click="emit('dataset',task.output_dataset_id)">查看生成版本 {{task.output_dataset_id.slice(0,8)}}</button>
    <button v-if="task.dataset_id!==dataset.id" type="button" class="text-button" @click="emit('dataset',task.dataset_id)">查看治理输入版本</button>
   </article>
  </div>
  <div class="pagination" aria-label="数据集历史分页"><button type="button" class="text-button" :disabled="page<=1||loading" @click="turn(-1)">上一页历史</button><span>第 {{page}} / {{Math.max(1,pages)}} 页 · {{total}} 个关联任务</span><button type="button" class="text-button" :disabled="!pages||page>=pages||loading" @click="turn(1)">下一页历史</button></div>
  <div v-if="reportTask" class="history-comparison" aria-live="polite">
   <h4>质量变化 · {{reportTask.slice(0,8)}}</h4><p v-if="reportLoading" class="hint">正在读取已保存的前后指标…</p><p v-if="reportError" class="alert" role="alert">{{reportError}}</p>
   <template v-if="selectedReport">
    <template v-if="change.kind==='ready'">
     <p class="hint">{{selectedReport.beforeRows}} 行 → {{selectedReport.afterRows}} 行。仅比较定义、参数和统计口径兼容的规则；各规则异常可能重叠，移除记录不等于修复原始值。</p>
     <IqrComparison :rows="change.rows"/>
     <button type="button" class="text-button" @click="emit('task',selectedReport.sourceTaskId)">查看对比来源任务</button>
     <div class="table-wrap"><table><thead><tr><th>规则 / 字段</th><th>之前</th><th>之后</th><th>减少</th></tr></thead><tbody><tr v-for="(row,index) in change.rows" :key="index"><td>{{ruleLabels[row.type]||row.type}} · {{row.columns.join(' + ')}}<small v-if="config(row)">{{config(row)}}</small></td><td>{{row.before}}</td><td>{{row.after}}</td><td :class="{'quality-worse':row.reducedBy<0}">{{row.reducedBy}}<small v-if="row.reducedBy<0">异常增加</small></td></tr></tbody></table></div>
    </template><p v-else class="hint">{{change.message}}</p>
   </template>
  </div>
 </section>
</template>
