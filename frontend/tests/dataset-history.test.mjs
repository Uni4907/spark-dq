import test from 'node:test';
import assert from 'node:assert/strict';
import {requestOwner,historyRole,comparisonState} from '../src/dataset-history.mjs';
test('late page/filter/dataset responses cannot replace current results',()=>{const owner=requestOwner(),old=owner.begin(),current=owner.begin();assert.equal(old(),false);assert.equal(current(),true);owner.invalidate();assert.equal(current(),false);});
test('unmounted history and report panes reject pending work',()=>{const owner=requestOwner(),current=owner.begin();owner.close();assert.equal(current(),false);assert.equal(owner.begin()(),false);});
test('input governance and output lineage remain distinct',()=>{assert.equal(historyRole({dataset_id:'a',governance_id:'g'},'a'),'以此版本治理');assert.equal(historyRole({dataset_id:'a',governance_id:'g'},'b'),'生成此版本');assert.equal(historyRole({dataset_id:'a'},'a'),'检测 / 复检此版本');});
test('failed tasks cannot display stale successful comparisons',()=>{assert.equal(comparisonState({task:{status:'FAILED'},comparison:[{reducedBy:4}]}).kind,'unavailable');});
test('incompatible and historical sources do not invent zero changes',()=>{assert.equal(comparisonState({task:{status:'SUCCEEDED'},comparisonUnavailable:'口径不兼容'}).message,'口径不兼容');assert.equal(comparisonState({task:{status:'SUCCEEDED'}}).kind,'unavailable');});
test('worsening and zero-row results survive unchanged',()=>{const rows=[{before:0,after:3,reducedBy:-3}];assert.deepEqual(comparisonState({task:{status:'SUCCEEDED'},beforeRows:0,comparison:rows}),{kind:'ready',rows});});
