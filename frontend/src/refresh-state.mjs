// Completed responses only replace changed data; failures leave the last good value intact.
export function stableValue(previous,next){return JSON.stringify(previous)===JSON.stringify(next)?previous:next;}
export const isActive=job=>['QUEUED','RUNNING','CANCELLING'].includes(job?.status);
export function pollDelay(active,hidden=false){return hidden?30000:active?3000:15000;}
// Schedule after completion, so a slow request cannot accumulate interval callbacks.
export function pollingLoop(run,delay,{schedule=setTimeout,cancel=clearTimeout,onError=()=>{}}={}){
 let stopped=true,timer=null,running=false;
 async function tick(){if(stopped||running)return;running=true;try{await run();}catch(error){onError(error);}finally{running=false;if(!stopped)timer=schedule(tick,delay());}}
 return {start(){if(!stopped)return;stopped=false;void tick();},reschedule(){if(!stopped&&!running){cancel(timer);timer=schedule(tick,delay());}},stop(){stopped=true;cancel(timer);}};
}
export async function settleRequests(requests){const results=await Promise.allSettled(requests);const failed=results.find(r=>r.status==='rejected');if(failed)throw failed.reason;}
