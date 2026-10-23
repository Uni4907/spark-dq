<script setup>
import {stableValue,isActive,pollDelay,pollingLoop,settleRequests} from './refresh-state.mjs';
import {ref,onMounted,onUnmounted} from 'vue';
const props=defineProps({api:Function});const emit=defineEmits(['dataset']);
const rows=ref([]),page=ref(1),pages=ref(0),total=ref(0),busy=ref(false),error=ref(''),pollError=ref(''),notice=ref(''),pending=ref(null);
const retryKeys=new Map();const labels={QUEUED:'排队中',RUNNING:'转换校验中',CANCELLING:'正在停止',CANCELLED:'已取消',SUCCEEDED:'已导入',FAILED:'导入失败'};
let disposed=false,loading=false,sequence=0;
async function refresh(force=false){if(loading&&!force)return;const seq=++sequence,p=page.value;loading=true;try{const result=await props.api('/imports/page?'+new URLSearchParams({page:p,pageSize:10}));if(disposed||seq!==sequence||p!==page.value)return;rows.value=stableValue(rows.value,result.items);pages.value=result.totalPages;total.value=result.total;pollError.value='';}catch(e){if(!disposed&&seq===sequence)pollError.value=e.message;}finally{if(seq===sequence)loading=false;}}
async function choose(event){const file=event.target.files[0];event.target.value='';if(!file)return;pending.value={file,key:crypto.randomUUID()};await send();}
async function send(){if(busy.value||!pending.value)return;busy.value=true;error.value='';try{const form=new FormData();form.append('file',pending.value.file);const job=await props.api('/imports',{method:'POST',headers:{'X-Import-Key':pending.value.key},body:form});pending.value=null;page.value=1;notice.value='导入已受理：'+job.id.slice(0,8)+'，完成后可选择数据集开始检测';await refresh(true);polling.reschedule();}catch(e){error.value=e.message+'；可用下方按钮重发同一次请求。';}finally{busy.value=false;}}
async function act(job,action){if(busy.value)return;busy.value=true;error.value='';try{const options={method:'POST'};if(action==='retry'){if(!retryKeys.has(job.id))retryKeys.set(job.id,crypto.randomUUID());options.headers={'X-Import-Key':retryKeys.get(job.id)};}await props.api('/imports/'+job.id+'/'+action,options);retryKeys.delete(job.id);page.value=1;notice.value=action==='cancel'?'取消请求已记录，停止后自动更新':'已使用保留的原文件创建独立导入任务';await refresh(true);polling.reschedule();}catch(e){error.value=e.message;}finally{busy.value=false;}}
async function turn(delta){page.value+=delta;await refresh(true);polling.reschedule();}
const polling=pollingLoop(()=>refresh(),()=>pollDelay(rows.value.some(isActive),document.hidden));
onMounted(()=>polling.start());onUnmounted(()=>{disposed=true;polling.stop();});
</script>
<template>
 <section class="card" style="margin-bottom:20px">
  <div class="section-title"><h2>原生 Parquet 导入</h2><label class="upload">{{busy?'处理中…':'选择 Parquet'}}<input aria-label="导入 Parquet" type="file" accept=".parquet" :disabled="busy||!!pending" @change="choose"></label></div>
  <p class="hint">单文件最多20 MiB，转换后最多20 MiB、100列、100万行。支持文本、布尔、整数、精确小数和1582-10-15起的日期；浮点、时间戳、嵌套类型及含回车的文本会拒绝。原文件独立保留；导入与检测共用队列。</p>
  <p v-if="error" class="alert" role="alert">{{error}}</p><p v-if="pollError" class="alert" role="alert">导入历史暂时无法更新：{{pollError}}</p><p v-if="notice" class="notice" role="status">{{notice}}</p>
  <p v-if="pending"><button :disabled="busy" @click="send">重发 {{pending.file.name}}（沿用导入标识）</button><button class="text-button" :disabled="busy" @click="pending=null;notice='已清除待重发请求，请先查看历史列表确认服务是否受理'">清除待重发请求</button></p>
  <div class="pagination"><button class="text-button" :disabled="page<=1" @click="turn(-1)">上一页导入</button><span>第 {{page}} / {{Math.max(1,pages)}} 页 · {{total}} 次导入 · 自动更新</span><button class="text-button" :disabled="!pages||page>=pages" @click="turn(1)">下一页导入</button></div>
  <p v-if="!rows.length" class="hint">暂无导入记录。可使用 CSV 上传入口。</p>
  <details v-for="job in rows" :key="job.id" style="margin:10px 0"><summary>{{job.name}} · {{labels[job.status]}} · {{job.id.slice(0,8)}}</summary>
   <p class="hint" style="overflow-wrap:anywhere">导入 {{job.id}} · {{job.source_bytes}} 字节<br>原文件 SHA-256 {{job.source_sha256}}<br v-if="job.manifest_sha256"><span v-if="job.manifest_sha256">转换清单 SHA-256 {{job.manifest_sha256}}</span></p>
   <p v-if="job.error" class="hint" style="white-space:pre-wrap;max-height:180px;overflow:auto">{{job.error}}</p>
   <p v-if="job.parent_import_id" class="hint">重试来源：{{job.parent_import_id}}</p>
   <button v-if="job.status==='SUCCEEDED'" class="secondary" :disabled="busy" @click="emit('dataset',job.dataset_id)">选择此数据集开始检测</button>
   <button v-if="['QUEUED','RUNNING'].includes(job.status)" class="secondary" :disabled="busy" @click="act(job,'cancel')">取消导入</button>
   <button v-if="['FAILED','CANCELLED'].includes(job.status)" class="secondary" :disabled="busy" @click="act(job,'retry')">使用原文件重新导入</button>
  </details>
 </section>
</template>
