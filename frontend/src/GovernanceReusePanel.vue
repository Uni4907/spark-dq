<script setup>
import {ref,onMounted,onUnmounted} from 'vue';
import {requestOwner} from './dataset-history.mjs';
const props=defineProps({taskId:String,api:Function,busy:Boolean});const emit=defineEmits(['apply']);
const query=ref(''),search=ref(''),page=ref(1),items=ref([]),total=ref(0),pages=ref(0),loading=ref(false),applying=ref(false),error=ref('');
const requests=requestOwner(),imports=requestOwner();
async function load(){
 const current=requests.begin();loading.value=true;error.value='';
 try{const result=await props.api('/governance-policies?'+new URLSearchParams({page:page.value,pageSize:5,q:search.value}));if(current()){items.value=result.items;total.value=result.total;pages.value=result.totalPages;}}
 catch(e){if(current())error.value=e.message;}finally{if(current())loading.value=false;}
}
async function find(){page.value=1;search.value=query.value.trim();await load();}
async function turn(delta){page.value+=delta;await load();}
async function apply(task){
 const current=imports.begin();applying.value=true;error.value='';
 try{const result=await props.api(`/governance-policies/${task.id}?`+new URLSearchParams({targetTaskId:props.taskId}));if(current())emit('apply',result);}
 catch(e){if(current())error.value=e.message;}finally{if(current())applying.value=false;}
}
onMounted(load);onUnmounted(()=>{requests.close();imports.close();});
</script>
<template>
 <details class="policy-reuse"><summary>复用历史治理策略</summary>
  <p class="hint">成功治理的配置已经随任务保存。选择后会替换下方表单，可修改或撤销；不会立即执行治理，也不会修改历史策略。仅复用治理配置，不复制检测规则；空值移除和去重仍按当前任务的字段与业务键执行。</p>
  <form class="search-row" @submit.prevent="find"><label :for="`policy-search-${taskId}`">查找数据集名称或任务编号<input :id="`policy-search-${taskId}`" v-model="query" type="search" maxlength="200" :disabled="busy||applying"></label><button type="submit" class="secondary" :disabled="busy||applying||loading">搜索历史策略</button></form>
  <p v-if="error" class="alert" role="alert">{{error}}</p><p v-if="loading" class="hint" role="status">正在读取可复用策略…</p>
  <p v-if="!loading&&!items.length&&!error" class="hint">{{search?'没有匹配的历史策略。':'还没有成功治理的策略；完成一次治理后即可复用。'}}</p>
  <ul v-if="items.length" class="policy-reuse-list"><li v-for="task in items" :key="task.id"><div><strong>{{task.dataset_name}}</strong><small>{{task.created_at}} · {{task.id.slice(0,8)}} · {{task.input_rows}} → {{task.output_rows}} 行</small></div><button type="button" class="text-button" :disabled="busy||loading||applying" @click="apply(task)">带入并替换当前配置</button><a :href="`/api/tasks/${task.id}/report.html`" target="_blank" rel="noopener">查看原治理报告</a></li></ul>
  <div class="pagination" aria-label="历史策略分页"><button type="button" class="text-button" :disabled="page<=1||busy||loading||applying" @click="turn(-1)">上一页策略</button><span>第 {{page}} / {{Math.max(1,pages)}} 页 · {{total}} 个策略</span><button type="button" class="text-button" :disabled="!pages||page>=pages||busy||loading||applying" @click="turn(1)">下一页策略</button></div>
 </details>
</template>
<style scoped>
.policy-reuse{padding:14px;border:1px solid #d5e4e7;background:#f5fafb;border-radius:8px}.policy-reuse-list{list-style:none;padding:0}.policy-reuse-list li{border-top:1px solid var(--border);padding:12px 0;display:flex;gap:12px;align-items:center;flex-wrap:wrap}.policy-reuse-list li>div{flex:1;min-width:180px}.policy-reuse-list small{display:block;color:#70848d}.policy-reuse-list a{color:var(--brand);font-size:12px}
</style>
