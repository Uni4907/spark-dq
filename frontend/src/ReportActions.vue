<script setup>
defineProps({task:{type:Object,required:true}});
</script>
<template>
 <section v-if="['SUCCEEDED','FAILED','CANCELLED'].includes(task.status)" class="report-actions" aria-label="报告导出">
  <h3>{{task.status==='SUCCEEDED'?'质量报告':'任务处理记录'}}</h3>
  <div class="report-links">
   <a class="secondary" :href="`/api/tasks/${task.id}/report.html`" target="_blank" rel="noopener">{{task.status==='SUCCEEDED'?'阅读 / 打印质量报告':'阅读 / 打印处理记录'}}</a>
   <a :href="`/api/tasks/${task.id}/report.html?download=true`" download>下载离线 HTML</a>
   <a :href="`/api/tasks/${task.id}/report`" download>下载 JSON</a>
  </div>
  <p class="hint">独立报告可离线阅读；打开后使用浏览器打印（Ctrl+P），可另存为 PDF。{{task.status==='SUCCEEDED'?'报告包含实际规则结果、可用的前后对比和治理审计。':'仅记录处理状态、原因和规则，不含成功的质量统计。'}}</p>
 </section>
</template>
