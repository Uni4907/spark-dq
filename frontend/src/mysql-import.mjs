// An uncertain POST retains its exact request and key until acknowledged or cleared.
export function importRequest(api,uuid=()=>crypto.randomUUID()){
 let pending=null,sending=false;
 return {
  get pending(){return pending;},
  async send(sourceId,columns){
   if(sending)throw new Error('正在提交，请稍候');
   if(!pending){if(!sourceId||!columns.length)throw new Error('请选择业务表及至少一个字段');pending={key:uuid(),body:{sourceId,columns:[...columns]}};}
   sending=true;
   try{const job=await api('/mysql-imports',{method:'POST',headers:{'Content-Type':'application/json','X-Import-Key':pending.key},body:JSON.stringify(pending.body)});if(!job?.id)throw new Error('服务返回的导入记录不完整');pending=null;return job;}finally{sending=false;}
  },
  clear(){if(!sending)pending=null;}
 };
}
export const importLabels={QUEUED:'排队中',RUNNING:'正在读取并校验快照',CANCELLING:'正在关闭业务连接',CANCELLED:'已取消',SUCCEEDED:'已导入',FAILED:'导入失败'};
const errors={SNAPSHOT_FAILED:'业务源连接或快照读取失败，请联系管理员检查只读连接。',ROW_LIMIT:'记录数超过此业务源允许的上限。',BYTE_LIMIT:'快照文件超过此业务源允许的大小。',TIME_LIMIT:'快照读取超过时间限制。',MYSQL_SNAPSHOT_IMPORT_FAILED:'快照导入失败，未发布数据集。',CANCELLED:'业务连接已关闭，未发布数据集。'};
export function importError(value){return errors[value]||value||'';}
export function selectionOf(job){try{const value=typeof job.source_request_json==='string'?JSON.parse(job.source_request_json):job.source_request_json;return value&&typeof value.sourceId==='string'&&Array.isArray(value.columns)?value:null;}catch{return null;}}
