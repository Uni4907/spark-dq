// Separate request ownership for history and report panes; late responses cannot
// update a different dataset, page, status filter, report, or unmounted panel.
export function requestOwner(){
 let generation=0,closed=false;
 return {begin(){const token=++generation;return ()=>!closed&&token===generation;},invalidate(){generation++;},close(){closed=true;generation++;}};
}
export const taskLabels={QUEUED:'排队中',RUNNING:'检测中',CANCELLING:'正在停止',SUCCEEDED:'已完成',FAILED:'失败',CANCELLED:'已取消'};
export const ruleLabels={MISSING:'空值',DUPLICATE:'重复',TYPE:'类型',RANGE:'数值范围',REGEX:'正则格式',IQR:'IQR数值异常',FD:'函数依赖冲突'};
export function historyRole(task,datasetId){return task.dataset_id===datasetId?(task.governance_id?'以此版本治理':'检测 / 复检此版本'):'生成此版本';}
export function comparisonState(report){
 if(report?.task?.status!=='SUCCEEDED')return {kind:'unavailable',message:'此任务没有成功的质量统计。'};
 if(report.comparison?.length)return {kind:'ready',rows:report.comparison};
 return {kind:'unavailable',message:report.comparisonUnavailable||'此任务没有可比较的成功来源；可查看完整报告中的检测结果。'};
}
