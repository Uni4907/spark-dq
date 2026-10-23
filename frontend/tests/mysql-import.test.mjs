import test from 'node:test';
import assert from 'node:assert/strict';
import {importRequest,selectionOf,importError} from '../src/mysql-import.mjs';
test('ambiguous network failure resends the frozen request and key',async()=>{
 const calls=[];const client=importRequest(async(path,options)=>{calls.push([path,options]);if(calls.length===1)throw new Error('timeout');return {id:'accepted'};},()=> 'stable-key');const columns=['金额','id'];
 await assert.rejects(client.send('allowed',columns));columns.pop();await client.send('different',['not_allowed']);assert.deepEqual(calls[0],calls[1]);assert.deepEqual(JSON.parse(calls[1][1].body),{sourceId:'allowed',columns:['金额','id']});assert.equal(client.pending,null);
});
test('concurrent clicks cannot create a second POST',async()=>{
 let finish;let posts=0;const client=importRequest(()=>{posts++;return new Promise(resolve=>finish=resolve);},()=> 'key');const first=client.send('s',['id']);await assert.rejects(client.send('s',['id']),/正在提交/);finish({id:'one'});await first;assert.equal(posts,1);
});
test('clearing pending state does not issue cancellation or silently retry',async()=>{
 let posts=0,keys=0;const client=importRequest(async()=>{posts++;throw new Error('disconnected');},()=>String(++keys));await assert.rejects(client.send('s',['id']));client.clear();assert.equal(client.pending,null);assert.equal(posts,1);await assert.rejects(client.send('s',['id']));assert.equal(client.pending.key,'2');
});
test('history tolerates invalid source JSON and explains bounded failure',()=>{assert.equal(selectionOf({source_request_json:'oops'}),null);assert.equal(selectionOf({source_request_json:'{}'}),null);assert.deepEqual(selectionOf({source_request_json:'{"sourceId":"s","columns":["id"]}'}),{sourceId:'s',columns:['id']});assert.match(importError('ROW_LIMIT'),/上限/);});
