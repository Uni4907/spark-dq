<script setup>
import {ref,watch,onMounted,onBeforeUnmount} from 'vue';
import * as echarts from 'echarts/core';
import {BarChart} from 'echarts/charts';
import {TooltipComponent,GridComponent} from 'echarts/components';
import {CanvasRenderer} from 'echarts/renderers';
import {createChartController} from './chart-controller.mjs';
echarts.use([BarChart,TooltipComponent,GridComponent,CanvasRenderer]);
const props=defineProps({rules:{type:Array,required:true},labels:{type:Object,required:true}});
const root=ref(null);const controller=createChartController(element=>echarts.init(element));let mounted=false,observer;
function redraw(){
 if(!mounted)return;const rows=props.rules.filter(rule=>rule.metrics);
 controller.update(root.value,{animation:false,tooltip:{renderMode:'richText',trigger:'axis'},grid:{left:45,right:20,bottom:65,top:20},xAxis:{type:'category',data:rows.map(rule=>(props.labels[rule.type]||rule.type)+' · '+rule.columns.join('+')),axisLabel:{interval:0,rotate:20}},yAxis:{type:'value',minInterval:1},series:[{type:'bar',data:rows.map(rule=>rule.metrics.status==='INSUFFICIENT_DATA'?null:(rule.metrics.blankCount??rule.metrics.duplicateRows??rule.metrics.conflictRows??rule.metrics.invalidCount)),itemStyle:{color:'#2563EB',borderRadius:[5,5,0,0]}}]});
}
const resize=()=>controller.resize();
onMounted(()=>{mounted=true;redraw();window.addEventListener('resize',resize);observer=new ResizeObserver(resize);observer.observe(root.value);});
watch(()=>props.rules,redraw,{deep:true,flush:'post'});
onBeforeUnmount(()=>{mounted=false;window.removeEventListener('resize',resize);observer?.disconnect();controller.dispose();});
</script>
<template><div ref="root" class="chart" aria-label="各规则异常行数图表"></div></template>
