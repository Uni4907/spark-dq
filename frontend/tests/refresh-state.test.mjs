import test from 'node:test';
import assert from 'node:assert/strict';
import {stableValue,isActive,pollDelay,pollingLoop,settleRequests} from '../src/refresh-state.mjs';
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
test('unchanged poll preserves nested identities; changed task state is published',()=>{
 const old={id:'a',status:'RUNNING',rules:[{id:'r',metrics:{invalidCount:1}}]};
 assert.equal(stableValue(old,structuredClone(old)),old);
 const done={...old,status:'SUCCEEDED'};assert.equal(stableValue(old,done),done);
 assert.equal(stableValue([],[]).length,0);
});
test('all active lifecycle states get fast polling without dropping idle or hidden polling',()=>{
 for(const status of ['QUEUED','RUNNING','CANCELLING']){assert.equal(isActive({status}),true);assert.equal(pollDelay(true),3000);}
 for(const status of ['SUCCEEDED','FAILED','CANCELLED'])assert.equal(isActive({status}),false);
 assert.equal(pollDelay(false),15000);assert.equal(pollDelay(true,true),30000);
});
test('slow request completes before scheduling next poll; stop rejects late scheduling',async()=>{
 const pending=deferred(),scheduled=[];let calls=0;const loop=pollingLoop(()=>{calls++;return pending.promise;},()=>3000,{schedule:(fn,ms)=>{scheduled.push({fn,ms});return 7;},cancel:()=>{}});
 loop.start();loop.start();assert.equal(calls,1);assert.equal(scheduled.length,0);
 pending.resolve();await pending.promise;await Promise.resolve();assert.equal(scheduled.length,1);assert.equal(scheduled[0].ms,3000);
 loop.stop();await scheduled[0].fn();assert.equal(calls,1);
});
test('failed refresh keeps last successful data, reports error, and schedules recovery',async()=>{
 const old=[{id:'a'}],scheduled=[],failures=[];let data=old;
 const loop=pollingLoop(async()=>{data=stableValue(data,await Promise.reject(new Error('offline')));},()=>15000,{schedule:(fn,ms)=>{scheduled.push({fn,ms});return 1;},cancel:()=>{},onError:e=>failures.push(e.message)});
 loop.start();await new Promise(r=>setImmediate(r));assert.equal(data,old);assert.deepEqual(failures,['offline']);assert.equal(scheduled.length,1);loop.stop();
});
test('aggregate failure waits for every in-flight request instead of opening another cycle',async()=>{
 const slow=deferred();let finished=false;const combined=settleRequests([Promise.reject(new Error('offline')),slow.promise]).catch(e=>{finished=true;return e.message;});
 await Promise.resolve();assert.equal(finished,false);slow.resolve();assert.equal(await combined,'offline');
});
test('reschedule uses new active cadence without concurrent requests',async()=>{
 const scheduled=[],cancelled=[];let active=false;const loop=pollingLoop(async()=>{},()=>pollDelay(active),{schedule:(fn,ms)=>{scheduled.push({fn,ms});return scheduled.length;},cancel:id=>cancelled.push(id)});
 loop.start();await new Promise(r=>setImmediate(r));assert.equal(scheduled.at(-1).ms,15000);active=true;loop.reschedule();assert.equal(scheduled.at(-1).ms,3000);assert.ok(cancelled.includes(1));loop.stop();
});
