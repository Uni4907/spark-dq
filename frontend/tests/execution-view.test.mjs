import test from 'node:test';
import assert from 'node:assert/strict';
import {taskView,serviceView} from '../src/execution-view.mjs';
test('queued or historical records never invent local execution',()=>{
 for(const status of ['QUEUED','SUCCEEDED']){const view=taskView({status,execution:{evidenceState:'ABSENT'},clusterExecution:{mode:'local'}});assert.equal(view.mode,'运行方式尚未记录');assert.equal(view.applicationId,null);}
});
test('unreachable cluster stays an attention state with a traceable application',()=>{
 const view=taskView({status:'CANCELLING',execution:{evidenceState:'STARTED',started:{applicationId:'app-1'}},clusterExecution:{mode:'standalone',cleanupObservation:'UNAVAILABLE_OR_INVALID_EVIDENCE'}});assert.equal(view.attention,true);assert.equal(view.applicationId,'app-1');assert.ok(!view.phase.includes('已完成'));
});
test('old clear observation cannot imply current business success',()=>{
 const task={status:'RUNNING',clusterExecution:{mode:'standalone',cleanupObservation:'CLEAR'}};assert.ok(taskView(task).phase.includes('业务结果仍在处理'));task.status='FAILED';assert.ok(taskView(task).phase.includes('任务结束时'));
});
test('damaged receipt exposes no unverified identities',()=>{
 const view=taskView({execution:{evidenceState:'INVALID',started:{applicationId:'untrusted'},submitted:{attemptId:'untrusted'}},clusterExecution:{mode:'standalone'}});assert.equal(view.applicationId,null);assert.equal(view.attemptId,null);assert.equal(view.attention,true);
});
test('only explicit service admission closes task controls',()=>{
 assert.equal(serviceView(null).blocked,false);assert.equal(serviceView({mode:'standalone',acceptingTasks:false}).blocked,true);assert.equal(serviceView({mode:'local',acceptingTasks:true}).blocked,false);
});
