<script setup>
import {ref,computed,watch} from 'vue';
import GovernanceReusePanel from './GovernanceReusePanel.vue';
import {policyForm} from './governance-policy.mjs';
const props=defineProps({task:Object,api:Function,busy:Boolean});
const emit=defineEmits(['submit','error']);
const columns=ref([]),loading=ref(false),fills=ref([]),normalizations=ref([]),mappings=ref([]),order=ref([]),dropMissing=ref(false),deduplicate=ref(false);
const previousPolicy=ref(null),reuseNotice=ref('');
const operations={TRIM:'去除首尾空格',UPPER:'转为大写',LOWER:'转为小写',TRIM_UPPER:'去除首尾空格并转大写',TRIM_LOWER:'去除首尾空格并转小写'};
const supported=computed(()=>props.task.snapshot?.semantics_version==='csv-v2');
const canDrop=computed(()=>props.task.rules.some(r=>r.type==='MISSING'));
const canDeduplicate=computed(()=>props.task.rules.some(r=>r.type==='DUPLICATE'));
const duplicateColumns=computed(()=>[fills.value,normalizations.value,mappings.value].some(rows=>new Set(rows.map(r=>r.column)).size!==rows.length));
const hasAction=computed(()=>dropMissing.value||deduplicate.value||fills.value.length||normalizations.value.length||mappings.value.length);
const invalidMappings=computed(()=>{
 const rows=mappings.value,all=rows.flatMap(r=>r.pairs);
 return rows.some(r=>!r.column||!r.pairs.length||r.pairs.length>50||new Set(r.pairs.map(p=>p.from)).size!==r.pairs.length)||
  all.length>200||all.reduce((n,p)=>n+p.from.length+p.to.length,0)>40000||
  all.some(p=>[p.from,p.to].some(v=>v.length>1000||v.includes('\0')||v==='\\N'));
});
const invalidOrder=computed(()=>order.value.length>5||new Set(order.value.map(r=>r.column)).size!==order.value.length||order.value.some(r=>!r.column));
watch(deduplicate,value=>{if(!value)order.value=[];});
const incomplete=computed(()=>fills.value.some(r=>!r.column||!r.value.trim()||r.value.length>1000)||normalizations.value.some(r=>!r.column));
watch(()=>props.task.id,async id=>{
 previousPolicy.value=null;reuseNotice.value='';columns.value=[];fills.value=[];normalizations.value=[];mappings.value=[];order.value=[];dropMissing.value=false;deduplicate.value=false;
 if(!supported.value)return;
 loading.value=true;
 try{const data=await props.api('/datasets/'+(props.task.governance?.output_dataset_id||props.task.dataset_id));if(props.task.id===id)columns.value=data.columns;}
 catch(e){if(props.task.id===id)emit('error',e.message);}
 finally{if(props.task.id===id)loading.value=false;}
},{immediate:true});
function addFill(){fills.value.push({column:columns.value[0],value:''});}
function addNormalization(){normalizations.value.push({column:columns.value[0],operation:'TRIM'});}
function addMapping(){mappings.value.push({column:columns.value[0],pairs:[{from:'',to:''}]});}
function addOrder(){order.value.push({column:columns.value.find(c=>!order.value.some(r=>r.column===c))||columns.value[0],type:'TEXT',direction:'DESC',nulls:'LAST'});}
function moveOrder(i,delta){const item=order.value.splice(i,1)[0];order.value.splice(i+delta,0,item);}
function currentPolicy(){return {policyVersion:order.value.length?4:mappings.value.length?3:2,dropMissing:dropMissing.value,deduplicate:deduplicate.value,
 ...(order.value.length?{deduplicationOrder:order.value.map(r=>({...r}))}:{}),
 fills:Object.fromEntries(fills.value.map(r=>[r.column,r.value])),
 normalizations:Object.fromEntries(normalizations.value.map(r=>[r.column,r.operation])),
 ...(mappings.value.length?{mappings:Object.fromEntries(mappings.value.map(r=>[r.column,r.pairs.map(p=>({from:p.from,to:p.to}))]))}:{})};}
function submit(){emit('submit',currentPolicy());}
function applyPolicy(policy){const form=policyForm(policy);dropMissing.value=form.dropMissing;deduplicate.value=form.deduplicate;fills.value=form.fills;normalizations.value=form.normalizations;mappings.value=form.mappings;order.value=form.order;}
function reusePolicy(result){previousPolicy.value=currentPolicy();applyPolicy(result.policy);reuseNotice.value='已带入 '+result.sourceDatasetName+' 的治理策略（任务 '+result.sourceTaskId.slice(0,8)+'）。可继续修改，历史策略保持不变。';}
function undoReuse(){if(previousPolicy.value){applyPolicy(previousPolicy.value);previousPolicy.value=null;reuseNotice.value='已恢复带入前的配置。';}}
</script>
<template>
 <details><summary>生成新的治理版本并自动复检</summary>
  <p v-if="!supported" class="hint">此历史任务保留旧 CSV 口径；请先对数据集新建检测任务，再配置治理。</p>
  <template v-else>
   <p class="hint">原文件与历史版本保留。顺序固定为：标准化 → 精确值映射 → 填充缺失值 → 移除仍缺失记录 → 去重 → 保存并复检。填充只影响缺失、空字符串和纯空格，其他值不变。</p>
   <GovernanceReusePanel :key="task.id" :task-id="task.id" :api="api" :busy="busy||loading" @apply="reusePolicy"/>
   <p v-if="reuseNotice" class="notice" role="status">{{reuseNotice}} <button v-if="previousPolicy" type="button" class="text-button" :disabled="busy" @click="undoReuse">撤销带入</button></p>
   <h3>文本标准化</h3>
   <div v-for="(row,i) in normalizations" :key="i" class="operation-row">
    <label>字段<select :aria-label="'标准化字段 '+(i+1)" v-model="row.column" :disabled="busy||loading"><option v-for="c in columns" :key="c" :value="c">{{c}}</option></select></label>
    <label>操作<select :aria-label="'标准化操作 '+(i+1)" v-model="row.operation" :disabled="busy||loading"><option v-for="(label,key) in operations" :key="key" :value="key">{{label}}</option></select></label>
    <button class="text-button" :disabled="busy" @click="normalizations.splice(i,1)">移除标准化配置</button>
   </div>
   <button class="text-button" :disabled="busy||loading||!columns.length" @click="addNormalization">添加文本标准化</button>
   <h3>精确值映射</h3>
   <p class="hint">将指定文本替换为统一值，例如“男”→“M”。区分大小写和空格；来源在标准化后的值上匹配，同一步不连锁替换。未匹配值和 NULL 保留，留空文本框表示空字符串。</p>
   <div v-for="(row,i) in mappings" :key="i" class="mapping-group">
    <label>映射字段<select :aria-label="'映射字段 '+(i+1)" v-model="row.column" :disabled="busy||loading"><option v-for="c in columns" :key="c" :value="c">{{c}}</option></select></label>
    <div v-for="(pair,j) in row.pairs" :key="j" class="operation-row">
     <label>来源值<textarea :aria-label="'映射来源 '+(i+1)+'-'+(j+1)" v-model="pair.from" maxlength="1000" rows="2" :disabled="busy"></textarea></label>
     <label>替换为<textarea :aria-label="'映射目标 '+(i+1)+'-'+(j+1)" v-model="pair.to" maxlength="1000" rows="2" :disabled="busy"></textarea></label>
     <button class="text-button" :disabled="busy" @click="row.pairs.splice(j,1)">移除映射项</button>
    </div>
    <button class="text-button" :disabled="busy||row.pairs.length>=50" @click="row.pairs.push({from:'',to:''})">添加映射项</button>
    <button class="text-button" :disabled="busy" @click="mappings.splice(i,1)">移除字段映射</button>
   </div>
   <button class="text-button" :disabled="busy||loading||!columns.length" @click="addMapping">添加字段映射</button>
   <p v-if="invalidMappings" class="alert">每字段须有 1～50 条、来源不能重复；总计最多 200 条、40000 字符。禁止保留空值标记和 NUL。</p>
   <h3>缺失值填充</h3>
   <div v-for="(row,i) in fills" :key="i" class="operation-row">
    <label>字段<select :aria-label="'填充字段 '+(i+1)" v-model="row.column" :disabled="busy||loading"><option v-for="c in columns" :key="c" :value="c">{{c}}</option></select></label>
    <label>填充值<textarea :aria-label="'填充值 '+(i+1)" v-model="row.value" maxlength="1000" rows="2" :disabled="busy" placeholder="仅填充缺失值，最多 1000 字符"></textarea></label>
    <button class="text-button" :disabled="busy" @click="fills.splice(i,1)">移除填充配置</button>
   </div>
   <button class="text-button" :disabled="busy||loading||!columns.length" @click="addFill">添加缺失值填充</button>
   <p class="hint">同一字段可先标准化再填充；映射目标和新增的填充值不会再次标准化，填充值也不会再次映射。数值字段请填写符合原检测规则的值。标准化业务键可能合并记录，去重按处理后的业务键执行。</p>
   <label class="check"><input type="checkbox" v-model="dropMissing" :disabled="busy||!canDrop">移除仍命中空值规则的整条记录</label>
   <label class="check"><input type="checkbox" v-model="deduplicate" :disabled="busy||!canDeduplicate">按本任务业务键去重</label>
   <template v-if="deduplicate">
    <h3>去重保留优先级</h3>
    <p class="hint">按以下顺序优先保留第一条，最多 5 级。数值按精确数值比较，日期仅支持 YYYY-MM-DD。NULL、空字符串和纯普通空格按所选空值位置排序；非空但不合法的数值/日期会停止本次治理，请先配置标准化或映射修正。</p>
    <div v-for="(row,i) in order" :key="i" class="priority-row">
     <label>第 {{i+1}} 优先字段<select :aria-label="'优先字段 '+(i+1)" v-model="row.column" :disabled="busy||loading"><option v-for="c in columns" :key="c" :value="c">{{c}}</option></select></label>
     <label>比较方式<select :aria-label="'比较方式 '+(i+1)" v-model="row.type" :disabled="busy"><option value="TEXT">文本字典序</option><option value="DECIMAL">精确数值</option><option value="DATE">日期 YYYY-MM-DD</option></select></label>
     <label>方向<select :aria-label="'优先方向 '+(i+1)" v-model="row.direction" :disabled="busy"><option value="DESC">较大 / 较新优先</option><option value="ASC">较小 / 较早优先</option></select></label>
     <label>空值位置<select :aria-label="'空值位置 '+(i+1)" v-model="row.nulls" :disabled="busy"><option value="LAST">空值最后</option><option value="FIRST">空值最前</option></select></label>
     <div><button class="text-button" :disabled="busy||i===0" @click="moveOrder(i,-1)" :aria-label="'上移优先级 '+(i+1)">上移</button><button class="text-button" :disabled="busy||i===order.length-1" @click="moveOrder(i,1)" :aria-label="'下移优先级 '+(i+1)">下移</button><button class="text-button" :disabled="busy" @click="order.splice(i,1)" :aria-label="'移除优先级 '+(i+1)">移除</button></div>
    </div>
    <button class="text-button" :disabled="busy||loading||order.length>=5||!columns.length" @click="addOrder">添加保留优先级</button>
    <p v-if="invalidOrder" class="alert">优先字段不能为空或重复，最多配置 5 级。</p>
   </template>
   <p class="hint">未配置优先级时，保持原全字段字典序保留规则；优先级全部并列时，也按全字段字典序保留，NULL 在前。原字段值不会因排序转换而改写。未配置空值/重复规则时，相应策略不可用。新版本可能为空。</p>
   <p v-if="duplicateColumns" class="alert">同一种操作中，每个字段只能配置一次。</p>
   <button class="primary" :disabled="busy||loading||!columns.length||!hasAction||incomplete||duplicateColumns||invalidMappings||invalidOrder" @click="submit">生成独立版本并复检</button>
  </template>
 </details>
</template>
<style scoped>
.priority-row{display:grid;grid-template-columns:2fr 2fr 2fr 1fr auto;gap:10px;align-items:end;margin:10px 0}.mapping-group{border:1px solid var(--border);border-radius:8px;padding:12px;margin:8px 0}.operation-row{display:grid;grid-template-columns:1fr 2fr auto;align-items:end;gap:10px;border:1px solid var(--border);border-radius:8px;padding:12px;margin:8px 0}
.operation-row label{font-size:12px}textarea{font:inherit;width:100%;display:block;padding:9px;border:1px solid #cbd5e1;border-radius:6px;margin-top:6px;resize:vertical}
@media(max-width:760px){.priority-row{grid-template-columns:1fr}.mapping-group{border:1px solid var(--border);border-radius:8px;padding:12px;margin:8px 0}.operation-row{grid-template-columns:1fr}}
</style>
