<script setup>
import {computed} from 'vue';
import {taskView} from './execution-view.mjs';
const props=defineProps({task:{type:Object,required:true}});
const view=computed(()=>taskView(props.task));
</script>
<template>
 <section class="execution-panel" :class="{attention:view.attention}" aria-label="执行与资源状态">
  <strong>{{view.mode}}</strong>
  <p role="status">{{view.phase}}</p>
  <p v-if="task.status==='CANCELLING'" class="hint">取消请求已记录。计算停止且资源核对完成后，任务才会显示“已取消”。</p>
  <details v-if="view.applicationId||view.attemptId"><summary>查看执行追溯编号</summary><p v-if="view.applicationId">Spark 作业编号：<code>{{view.applicationId}}</code></p><p v-if="view.attemptId">本次执行编号：<code>{{view.attemptId}}</code></p><p class="hint">编号用于追溯本次计算，不单独代表业务成功。</p></details>
 </section>
</template>
<style scoped>
.execution-panel{margin:16px 0;padding:14px 16px;border:1px solid #dbeafe;border-radius:8px;background:#eff6ff}.execution-panel p{margin:6px 0}.execution-panel.attention{background:#fff9eb;border-color:#ecd7a7}.execution-panel code{overflow-wrap:anywhere}.execution-panel details{margin-top:8px}
</style>
