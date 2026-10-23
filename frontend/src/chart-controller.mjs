export function createChartController(create){
 let instance=null,element=null,disposed=false,signature=null;
 function reset(){const previous=instance;instance=null;element=null;signature=null;if(previous&&!previous.isDisposed?.())previous.dispose();}
 return {
  update(target,options){if(disposed)return;if(!target){reset();return;}if(target!==element){reset();element=target;instance=create(target);}const next=JSON.stringify(options);if(next!==signature){instance.setOption(options,true);signature=next;}},
  resize(){if(!disposed&&instance&&!instance.isDisposed?.())instance.resize();},
  dispose(){if(disposed)return;disposed=true;reset();}
 };
}
