<script setup>
import {stableValue,isActive,pollDelay,pollingLoop,settleRequests} from './refresh-state.mjs';
import {ref,computed,onMounted,onUnmounted} from 'vue';
import {requestOwner} from './dataset-history.mjs';
import {importRequest,importLabels,importError,selectionOf} from './mysql-import.mjs';
const props=defineProps({api:{type:Function,required:true}}),emit=defineEmits(['dataset']);
const sources=ref([]),sourceId=ref(''),columns=ref([]),sourcesLoading=ref(false),sourcesReady=ref(false),sourceError=ref('');
const rows=ref([]),page=ref(1),pages=ref(0),total=ref(0),loading=ref(false),busy=ref(false),error=ref(''),historyError=ref(''),trackError=ref(''),notice=ref(''),pending=ref(false),tracked=ref(null);
const source=computed(()=>sources.value.find(s=>s.id===sourceId.value));
const request=importRequest((...args)=>props.api(...args)),owner=requestOwner(),sourceOwner=requestOwner(),trackOwner=requestOwner();
let disposed=false,selectedImport=null,requesting=false,ready=false,tracking=false;
function choose(){columns.value=(source.value?.columns||[]).map(c=>c.source);}
async function loadSources(){
 const current=sourceOwner.begin();sourcesLoading.value=true;sourceError.value='';
 try{const result=await props.api('/mysql-sources');if(!current())return;sources.value=result;sourcesReady.value=true;if(!result.some(s=>s.id===sourceId.value)){sourceId.value=result[0]?.id||'';choose();}}
 catch(e){if(current())sourceError.value=e.message;}finally{if(current())sourcesLoading.value=false;}
}
function completed(job){if(job?.status==='SUCCEEDED'&&job.dataset_id&&job.id!==selectedImport){selectedImport=job.id;notice.value='业务快照已注册。使用“选择本次快照数据集”继续，当前选择与规则草稿保持保留。';}}
async function refresh(force=false){
 if(requesting&&!force)return;
 const current=owner.begin(),p=page.value;requesting=true;loading.value=!ready||force;
 try{const result=await props.api('/mysql-imports/page?'+new URLSearchParams({page:p,pageSize:5}));if(!current())return;if(result.totalPages&&p>result.totalPages){page.value=result.totalPages;loading.value=false;requesting=false;await refresh(true);polling.reschedule();return;}rows.value=stableValue(rows.value,result.items);ready=true;total.value=result.total;pages.value=result.totalPages;historyError.value='';}
 catch(e){if(current())historyError.value=e.message;}finally{if(current()){loading.value=false;requesting=false;}}
}
async function pollTracked(){
 const id=tracked.value?.id;if(tracking||!id||!['QUEUED','RUNNING','CANCELLING'].includes(tracked.value.status))return;
 tracking=true;const current=trackOwner.begin();
 try{const job=await props.api('/mysql-imports/'+id);if(current()&&tracked.value?.id===id){tracked.value=job;trackError.value='';completed(job);}}
 catch(e){if(current())trackError.value=e.message;}finally{tracking=false;}
}
async function send(){
 if(busy.value)return;busy.value=true;error.value='';
 try{const job=await request.send(sourceId.value,columns.value);if(disposed)return;trackOwner.invalidate();tracked.value=job;page.value=1;notice.value='快照任务已受理。仅显示处理状态，不估算完成百分比。';completed(job);await refresh(true);polling.reschedule();}
 catch(e){if(!disposed)error.value=e.message+'；若响应不确定，可沿用标识重发，避免重复创建。';}
 finally{pending.value=!!request.pending;busy.value=false;}
}
async function cancel(job){
 if(busy.value)return;busy.value=true;error.value='';
 try{const result=await props.api('/mysql-imports/'+job.id+'/cancel',{method:'POST'});if(disposed)return;if(tracked.value?.id===job.id){trackOwner.invalidate();tracked.value=result;}notice.value='取消已受理。业务连接关闭后才显示已取消，未发布半成品。';await refresh(true);polling.reschedule();}
 catch(e){if(!disposed)error.value=e.message;}finally{busy.value=false;}
}
function clearPending(){request.clear();pending.value=false;notice.value='待重发请求已清除；这不会取消服务端任务，请先检查导入历史。';}
async function turn(delta){page.value=Math.max(1,page.value+delta);await refresh(true);polling.reschedule();}
const polling=pollingLoop(()=>settleRequests([refresh(),pollTracked()]),()=>pollDelay(rows.value.some(isActive)||isActive(tracked.value),document.hidden));
onMounted(()=>{loadSources();polling.start();});
onUnmounted(()=>{disposed=true;polling.stop();owner.close();sourceOwner.close();trackOwner.close();});
</script>
<template>
 <section class="card mysql-import" aria-label="MySQL 业务数据源">
  <div class="section-title"><h2>MySQL 业务快照</h2><button type="button" class="text-button" :disabled="sourcesLoading||busy||pending" @click="loadSources">刷新允许的业务表</button></div>
  <p class="hint">从管理员允许的业务表读取只读快照。检测、治理及复检使用已保存的快照，不改写业务库；再次导入会读取新的数据。</p>
  <p v-if="sourceError" role="alert" class="alert">业务表加载失败：{{sourceError}}</p><p v-if="sourcesLoading" role="status">正在读取允许的业务表…</p>
  <p v-if="sourcesReady&&!sources.length" class="hint">尚未配置可用的 MySQL 业务源，请联系管理员。仍可使用 CSV 或 Parquet。</p>
  <fieldset v-if="sources.length" :disabled="busy||pending||sourcesLoading">
   <label for="mysql-source">允许的业务表</label><select id="mysql-source" v-model="sourceId" @change="choose"><option v-for="item in sources" :key="item.id" :value="item.id">{{item.schema}}.{{item.table}} · {{item.id}}</option></select>
   <template v-if="source"><p class="hint">最多 {{source.limits.rows}} 行 · {{source.limits.bytes}} 字节 · {{source.limits.seconds}} 秒。导入与检测共用队列。</p><div><strong>选择快照字段</strong><button type="button" class="text-button" @click="choose">选择全部业务字段</button><button type="button" class="text-button" @click="columns=[]">清空业务字段选择</button></div>
    <div class="mysql-fields"><label v-for="field in source.columns" :key="field.source" class="check"><input type="checkbox" v-model="columns" :value="field.source">{{field.source}}{{field.source!==field.target?' → '+field.target:''}}</label></div>
   </template>
  </fieldset>
  <p v-if="error" class="alert" role="alert">{{error}}</p><p v-if="historyError||trackError" class="alert" role="alert">快照状态暂时无法更新：{{historyError||trackError}}</p><p v-if="notice" class="notice" role="status">{{notice}}</p>
  <button v-if="!pending&&sources.length" type="button" class="primary" :disabled="busy||!columns.length||!source" @click="send">创建 MySQL 快照</button>
  <template v-if="pending"><button type="button" :disabled="busy" @click="send">沿用标识重发快照请求</button><button type="button" class="text-button" :disabled="busy" @click="clearPending">清除待重发请求</button></template>
  <div v-if="tracked" class="mysql-active" aria-label="本次快照状态"><strong>本次快照 · {{importLabels[tracked.status]||'状态待确认'}}</strong><progress v-if="['QUEUED','RUNNING','CANCELLING'].includes(tracked.status)" aria-label="业务快照处理中"></progress><p class="hint">{{tracked.id}}</p><p v-if="tracked.error" class="alert">{{importError(tracked.error)}}</p><button v-if="['QUEUED','RUNNING'].includes(tracked.status)" type="button" class="secondary" :disabled="busy" @click="cancel(tracked)">取消本次快照</button><button v-if="tracked.status==='SUCCEEDED'" type="button" class="secondary" @click="emit('dataset',tracked.dataset_id)">选择本次快照数据集</button></div>
  <details open><summary>MySQL 快照导入历史 · {{total}} 次</summary>
   <button type="button" class="text-button" :disabled="loading" @click="refresh(true)">刷新快照历史</button><p v-if="!rows.length&&!loading" class="hint">暂无业务快照导入记录。</p>
   <details v-for="job in rows" :key="job.id" class="mysql-job"><summary>{{job.name}} · {{importLabels[job.status]||'状态待确认'}} · {{job.id.slice(0,8)}}</summary><p class="hint">导入编号 {{job.id}} · {{job.created_at}}</p><p v-if="selectionOf(job)" class="hint">业务源 {{selectionOf(job).sourceId}} · 字段 {{selectionOf(job).columns.join('、')}}</p><p v-if="job.error" class="alert">{{importError(job.error)}}</p><p v-if="job.source_sha256" class="hint">快照 {{job.source_bytes}} 字节 · SHA-256 {{job.source_sha256}}</p><button v-if="job.status==='SUCCEEDED'&&job.dataset_id" type="button" class="secondary" @click="emit('dataset',job.dataset_id)">选择此业务快照数据集</button><button v-if="['QUEUED','RUNNING'].includes(job.status)" type="button" class="secondary" :disabled="busy" @click="cancel(job)">取消此业务快照</button><p v-if="['FAILED','CANCELLED'].includes(job.status)" class="hint">未发布数据集。重新导入会读取新的业务快照；旧记录继续保留。</p></details>
   <div class="pagination" aria-label="MySQL 导入分页"><button type="button" class="text-button" :disabled="page<=1||loading" @click="turn(-1)">上一页业务快照</button><span>第 {{page}} / {{Math.max(1,pages)}} 页 · 自动更新</span><button type="button" class="text-button" :disabled="!pages||page>=pages||loading" @click="turn(1)">下一页业务快照</button></div>
  </details>
 </section>
</template>
<style scoped>
.mysql-import{margin-bottom:20px}.mysql-fields{display:flex;gap:12px;flex-wrap:wrap;margin:12px 0}.mysql-active{background:#eff6ff;padding:14px;margin-top:16px;border-radius:8px}.mysql-job{margin:12px 0}.mysql-import p{overflow-wrap:anywhere}.mysql-active progress{display:block;width:100%;margin-top:10px}
</style>
