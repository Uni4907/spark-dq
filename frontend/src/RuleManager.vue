<script setup>
import {ref,computed,watch} from 'vue';
const props=defineProps({columns:Array,baseRules:Array,api:Function,plan:Object});
const emit=defineEmits(['update:plan','error','busy']);
const templates=ref([]),selected=ref(''),loaded=ref(null),draft=ref([]),extra=ref([]),history=ref([]),name=ref(''),busy=ref(false),message=ref('');
const labels={MISSING:'空值',DUPLICATE:'重复业务键',TYPE:'类型',RANGE:'数值范围',REGEX:'正则格式',IQR:'IQR数值异常',FD:'函数依赖冲突'};
function clean(rows){return rows.map(r=>({type:r.type,columns:[...r.columns],params:r.type==='RANGE'?Object.fromEntries(Object.entries(r.params||{}).filter(([,v])=>v!==''&&v!==null)):r.params||{},definitionVersion:1}));}
const dirty=computed(()=>loaded.value&&JSON.stringify(clean(draft.value))!==JSON.stringify(clean(loaded.value.rules)));
const rows=computed(()=>selected.value?draft.value:extra.value);
const definitions=computed(()=>selected.value?clean(draft.value):[...props.baseRules,...clean(extra.value)]);
const plan=computed(()=>selected.value?{templateId:selected.value,templateRevision:loaded.value?.revision,valid:!!loaded.value&&!dirty.value}:{rules:definitions.value,valid:true});
watch(plan,value=>emit('update:plan',value),{immediate:true,deep:true});
async function run(fn){busy.value=true;emit('busy',true);message.value='';try{await fn();}catch(e){emit('error',e.message);}finally{busy.value=false;emit('busy',false);}}
async function refresh(){templates.value=await props.api('/rule-templates');}
async function load(revision){await run(async()=>{
 if(!selected.value){loaded.value=null;draft.value=[];history.value=[];return;}
 loaded.value=null;draft.value=[];
 loaded.value=await props.api('/rule-templates/'+selected.value+(revision?'?revision='+revision:''));
 draft.value=JSON.parse(JSON.stringify(loaded.value.rules));name.value=loaded.value.name;
 history.value=await props.api('/rule-templates/'+selected.value+'/versions');
});}
function changeType(row){row.params=row.type==='TYPE'?{dataType:'INTEGER'}:row.type==='IQR'?{k:'1.5'}:row.type==='FD'?{dependentColumns:props.columns.filter(c=>!row.columns.includes(c)).slice(0,1)}:{};if(!['DUPLICATE','FD'].includes(row.type))row.columns=row.columns.slice(0,1);}
function add(){rows.value.push({type:'TYPE',columns:[props.columns[0]],params:{dataType:'INTEGER'}});}
async function reload(){await refresh();if(selected.value)await load();}
async function save(update=false){await run(async()=>{
 const body={name:name.value,rules:definitions.value,...(update?{expectedRevision:loaded.value.revision}:{})};
 const saved=await props.api('/rule-templates'+(update?'/'+selected.value:''),{method:update?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
 await refresh();selected.value=saved.template_id;loaded.value=saved;draft.value=JSON.parse(JSON.stringify(saved.rules));history.value=await props.api('/rule-templates/'+selected.value+'/versions');
 message.value='已保存模板修订 '+saved.revision+'；历史任务不受影响';
});}
async function archive(){await run(async()=>{
 await props.api('/rule-templates/'+selected.value+'?revision='+loaded.value.revision,{method:'DELETE'});
 selected.value='';loaded.value=null;draft.value=[];history.value=[];await refresh();message.value='模板已归档，历史修订和任务快照保留';
});}
run(refresh);
</script>
<template>
 <details open class="rule-manager"><summary>高级检测规则与模板</summary>
  <details class="rule-semantics"><summary>检测语义、空值处理与精度边界</summary>  <p class="hint">类型、数值范围、正则规则跳过缺失值、空字符串和纯空格；空值请用空值规则检测。规则不自动修改数据。IQR 使用精确连续四分位数，至少4个有效值；非数值及超出精度域的值跳过并单列。每次复检重新拟合阈值。FD验证配置的X→Y，任一参与字段空白则跳过，冲突组全部参与行标记；不自动推断依赖或修复。</p></details>
  <label class="form-label">规则来源<select aria-label="规则模板" v-model="selected" :disabled="busy" @change="load()"><option value="">手动配置：基本规则 + 附加规则</option><option v-for="t in templates" :key="t.id" :value="t.id">{{t.name}} · 修订 {{t.revision}}</option></select></label>
  <button class="text-button" :disabled="busy" @click="run(reload)">刷新模板和修订</button>
  <p v-if="loaded" class="hint">模板模式使用下方完整规则，忽略上方字段勾选。<label>历史修订 <select aria-label="模板历史修订" :value="loaded.revision" :disabled="busy" @change="load($event.target.value)"><option v-for="h in history" :key="h.revision" :value="h.revision">{{h.revision}} · {{h.name}}</option></select></label></p>
  <div class="rule-row" v-for="(r,i) in rows" :key="i">
   <label>规则类型<select :aria-label="'规则类型 '+(i+1)" v-model="r.type" :disabled="busy" @change="changeType(r)"><option v-for="type in (selected?['MISSING','DUPLICATE','TYPE','RANGE','REGEX','IQR','FD']:['TYPE','RANGE','REGEX','IQR','FD'])" :key="type" :value="type">{{labels[type]}}</option></select></label>
   <label>字段<select v-if="['DUPLICATE','FD'].includes(r.type)" :aria-label="'规则字段 '+(i+1)" multiple v-model="r.columns" :disabled="busy"><option v-for="c in columns" :key="c" :value="c">{{c}}</option></select><select v-else :aria-label="'规则字段 '+(i+1)" :value="r.columns[0]" :disabled="busy" @change="r.columns=[$event.target.value]"><option v-for="c in columns" :key="c" :value="c">{{c}}</option></select></label>
   <label v-if="r.type==='TYPE'">目标类型<select :aria-label="'目标类型 '+(i+1)" v-model="r.params.dataType" :disabled="busy"><option value="INTEGER">64 位整数</option><option value="DECIMAL">十进制数</option><option value="DATE">日期 YYYY-MM-DD</option></select></label>
   <template v-if="r.type==='RANGE'"><label>最小值（含）<input :aria-label="'最小值 '+(i+1)" v-model="r.params.min" placeholder="留空表示无下界" :disabled="busy"></label><label>最大值（含）<input :aria-label="'最大值 '+(i+1)" v-model="r.params.max" placeholder="留空表示无上界" :disabled="busy"></label></template>
   <label v-if="r.type==='FD'">依赖项 Y（上方字段为决定项 X）<select :aria-label="'依赖项Y '+(i+1)" multiple v-model="r.params.dependentColumns" :disabled="busy"><option v-for="c in columns.filter(c=>!r.columns.includes(c))" :key="c" :value="c">{{c}}</option></select></label>
   <label v-if="r.type==='IQR'">IQR 倍数 k（0～10，不含0）<input :aria-label="'IQR倍数 '+(i+1)" v-model="r.params.k" :disabled="busy"></label>
   <label v-if="r.type==='REGEX'">正则（整字段匹配）<input :aria-label="'正则表达式 '+(i+1)" v-model="r.params.pattern" maxlength="256" :disabled="busy"></label>
   <p v-if="r.type==='IQR'" class="hint rule-explanation">数值字段 · 精确连续四分位数 · 至少4个有效值 · 每次复检重新拟合；空值、非数值和超精度域分别统计。</p><p v-if="r.type==='FD'" class="hint rule-explanation">决定项 X → 依赖项 Y；精确比较原文，任一参与字段空白则跳过，冲突组全部参与行标记。</p>
   <button class="text-button" :disabled="busy" @click="rows.splice(i,1)">移除此规则</button>
  </div>
  <button class="text-button" :disabled="busy" @click="add">添加检测规则</button>
  <p class="hint">十进制值最多 28 位整数、10 位小数，不接受科学计数法或前后空格。正则使用 Java 语法，最多 256 字符。</p>
  <label class="form-label">模板名称<input aria-label="模板名称" v-model="name" maxlength="120" :disabled="busy"></label>
  <button class="text-button" :disabled="busy||!name.trim()" @click="save(false)">保存为新模板</button>
  <button v-if="loaded" class="text-button" :disabled="busy||!name.trim()" @click="save(true)">保存新修订</button>
  <button v-if="loaded" class="text-button" :disabled="busy" @click="archive">归档模板</button>
  <p v-if="dirty" class="hint">规则已有未保存修改，请保存修订后再用该模板检测。旧修订发生并发冲突时会要求刷新。</p>
  <p v-if="message" class="notice" role="status">{{message}}</p>
 </details>
</template>
<style scoped>
.rule-row{border:1px solid var(--border);border-radius:8px;padding:10px;margin:10px 0;display:grid;gap:8px}
.rule-row label{font-size:12px}input{font:inherit;padding:9px;border:1px solid #cbd5e1;border-radius:6px;width:100%;margin-top:5px}
</style>
