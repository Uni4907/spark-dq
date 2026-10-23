export function serviceView(service){
 if(!service)return {label:'运行方式待确认',attention:true,blocked:false};
 return {label:service.mode==='standalone'?'集群计算':service.mode==='local'?'本地计算':'运行方式待确认',attention:service.acceptingTasks===false,blocked:service.acceptingTasks===false};
}
export function taskView(task){
 const receipt=task?.execution||{},cluster=task?.clusterExecution||{};
 const master=receipt.submitted?.sparkMaster||'';
 const remote=cluster.mode==='standalone'||master.startsWith('spark://');
 const local=master.startsWith('local')&&receipt.evidenceState!=='INVALID';
 const terminal=['SUCCEEDED','FAILED','CANCELLED'].includes(task?.status);
 let phase='等待执行记录',attention=false;
 if(receipt.evidenceState==='INVALID'){phase='执行记录暂时无法核验，未据此判断任务结果';attention=true;}
 else if(remote){
  const states={PENDING:'计算结束后还会核对资源释放',AWAITING_SECOND_CLEAR_OBSERVATION:'计算已停止，正在再次核对资源',REMOTE_RESOURCES_PRESENT:'仍有计算资源未退出，正在等待释放',UNAVAILABLE_OR_INVALID_EVIDENCE:'暂时无法确认资源状态，系统会继续核对',REQUIRES_ORIGINAL_CLUSTER_PROFILE:'需要恢复原集群配置后继续核对',INVALID:'资源核对记录暂时不可用'};
  phase=cluster.cleanupObservation==='CLEAR'?(terminal?'任务结束时已完成资源释放核对':'已记录资源核对，业务结果仍在处理'):states[cluster.cleanupObservation]||'正在核对执行状态';
  attention=['REMOTE_RESOURCES_PRESENT','UNAVAILABLE_OR_INVALID_EVIDENCE','REQUIRES_ORIGINAL_CLUSTER_PROFILE','INVALID'].includes(cluster.cleanupObservation);
 }else if(local)phase=terminal?'已保留本次执行记录':'正在本地准备或计算';
 else if(terminal)phase='此历史任务未记录运行方式';
 return {mode:remote?'集群计算':local?'本地计算':'运行方式尚未记录',phase,attention,applicationId:receipt.evidenceState==='INVALID'?null:receipt.started?.applicationId||null,attemptId:receipt.evidenceState==='INVALID'?null:receipt.submitted?.attemptId||null};
}
