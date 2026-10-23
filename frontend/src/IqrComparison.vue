<script setup>
defineProps({rows:{type:Array,required:true}});
const number=(metrics,key)=>metrics?.[key]??'未计算';
</script>
<template>
 <section v-for="(row,i) in rows.filter(r=>r.type==='IQR')" :key="i" class="iqr-comparison" aria-label="IQR 精确阈值对比">
  <h3>IQR 精确阈值 · {{row.columns.join(' + ')}}</h3>
  <p class="hint">每次检测独立拟合阈值。有效值不足4个不作正常结论，异常数下降不等于原始值被修复。</p>
  <div class="table-wrap"><table><thead><tr><th>检测阶段</th><th>Q1</th><th>Q3</th><th>IQR</th><th>下界</th><th>上界</th></tr></thead><tbody><tr v-for="item in [{name:'治理前',metrics:row.beforeMetrics},{name:'治理后 / 复检',metrics:row.afterMetrics}]" :key="item.name"><td>{{item.name}}</td><td v-for="key in ['q1','q3','iqr','lowerBound','upperBound']" :key="key">{{number(item.metrics,key)}}</td></tr></tbody></table></div>
  <details><summary>完整指标、有效值与跳过原因</summary><pre>之前 {{row.beforeMetrics}}
之后 {{row.afterMetrics}}</pre></details>
 </section>
</template>
