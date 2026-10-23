import test from 'node:test';
import assert from 'node:assert/strict';
import {createChartController} from '../src/chart-controller.mjs';
function instance(){return {sets:0,stops:0,resizes:0,setOption(){assert.equal(this.stops,0);this.sets++;},resize(){assert.equal(this.stops,0);this.resizes++;},dispose(){this.stops++;},isDisposed(){return this.stops>0;}};}
test('switching chart elements disposes the previous instance once',()=>{
 const instances=[];const controller=createChartController(()=>{const item=instance();instances.push(item);return item;});const first={},second={};controller.update(first,{});controller.update(first,{});controller.update(second,{});assert.equal(instances.length,2);assert.equal(instances[0].stops,1);assert.equal(instances[0].sets,1);assert.equal(instances[1].sets,1);
});
test('late render or resize cannot revive an unmounted chart',()=>{
 const chart=instance();let creates=0;const controller=createChartController(()=>{creates++;return chart;});controller.update({},{});controller.dispose();controller.update({},{});controller.resize();controller.dispose();assert.equal(creates,1);assert.equal(chart.sets,1);assert.equal(chart.stops,1);assert.equal(chart.resizes,0);
});
test('missing element releases chart without retaining a stale instance',()=>{
 const charts=[];const controller=createChartController(()=>{const chart=instance();charts.push(chart);return chart;});controller.update({},{});controller.update(null,{});controller.update(null,{});controller.update({},{});assert.equal(charts.length,2);assert.equal(charts[0].stops,1);
});

test('unchanged options skip redraw; changed metrics update the same canvas instance',()=>{
 const chart=instance(),target={};let creates=0;const controller=createChartController(()=>{creates++;return chart;});controller.update(target,{series:[{data:[1,4]}]});controller.update(target,{series:[{data:[1,4]}]});assert.equal(chart.sets,1);controller.update(target,{series:[{data:[0,4]}]});assert.equal(chart.sets,2);assert.equal(creates,1);assert.equal(chart.stops,0);
});
