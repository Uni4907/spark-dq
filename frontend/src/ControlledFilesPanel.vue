<script setup>
import {ref,onMounted,onUnmounted} from 'vue';
const props=defineProps({api:{type:Function,required:true}}),emit=defineEmits(['dataset']);
const files=ref([]),selected=ref(''),busy=ref(false),error=ref(''),notice=ref('');let closed=false;
async function refresh(){error.value='';try{const rows=await props.api('/controlled-files');if(closed)return;files.value=rows;if(!rows.some(f=>f.id===selected.value))selected.value=rows[0]?.id||'';}catch(e){if(!closed)error.value=e.message;}}
async function register(){if(busy.value)return;busy.value=true;error.value='';try{const data=await props.api('/controlled-files/'+encodeURIComponent(selected.value)+'/register',{method:'POST'});if(closed)return;notice.value='受控快照已注册并选中；相同来源及指纹会复用已有数据集。';emit('dataset',data.id);}catch(e){if(!closed)error.value=e.message+'；响应不确定时，可稍后重试同一来源。';}finally{busy.value=false;}}
onMounted(refresh);onUnmounted(()=>closed=true);
</script>
<template>
 <section class="card" aria-label="受控预置文件" style="margin-bottom:20px"><div class="section-title"><h2>预置大文件</h2><button type="button" class="text-button" :disabled="busy" @click="refresh">刷新预置文件</button></div>
  <p class="hint">由管理员预先放置并固定指纹的 CSV，不通过浏览器传输文件正文。独立档位最多64MiB、100万行；普通上传仍为20MiB。治理版本沿用此档位，异常与中间输出受预算约束；填充膨胀等操作仍可能因上限失败。</p>
  <p v-if="!files.length&&!error" class="hint">尚未配置受控预置文件，其他导入入口仍可使用。</p><label v-if="files.length" for="controlled-file">允许注册的文件</label><select v-if="files.length" id="controlled-file" v-model="selected" :disabled="busy"><option v-for="file in files" :key="file.id" :value="file.id">{{file.name}} · {{file.id}}</option></select><button v-if="files.length" class="primary" type="button" :disabled="busy||!selected" @click="register">{{busy?'正在校验并登记…':'注册预置快照'}}</button>
  <p v-if="error" class="alert" role="alert">{{error}}</p><p v-if="notice" class="notice" role="status">{{notice}}</p>
 </section>
</template>
