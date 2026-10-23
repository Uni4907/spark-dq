<script setup>
defineProps({dataset:{type:Object,required:true}});
</script>
<template>
 <details v-if="dataset.source" aria-label="数据集来源追溯"><summary>{{dataset.source.format==='MYSQL_SNAPSHOT'?'MySQL 业务快照来源':'文件来源'}} · {{dataset.source.files?.length||0}} 份文件</summary>
  <template v-if="dataset.source.format==='MYSQL_SNAPSHOT'"><p class="hint">业务源 {{dataset.source.sourceId}} · 表 {{dataset.source.schema}}.{{dataset.source.table}}<br>快照导入编号 {{dataset.source.importId}}<br>已保存的只读快照不会随业务表变化；复检不重新访问业务库。</p><ul><li v-for="field in dataset.source.mapping" :key="field.source">{{field.source}} → {{field.target}}</li></ul></template>
  <p v-else-if="dataset.source.format==='CONTROLLED_CSV'" class="hint">受控64MiB/100万行档位 · {{dataset.source.kind==='PRESET'?'预置来源 '+dataset.source.sourceId:'治理生成版本，父版本 '+dataset.source.parentDatasetId}}。原文与指纹保留；此版本不重新读取预置文件。</p>
  <p v-else class="hint">{{dataset.source.format==='PARQUET_NATIVE'?'原生 Parquet 经精确类型转换及完整数据核验，检测和版本下载使用转换后的 CSV。':'按提交顺序合并；各原文件与合并 CSV 均保留。'}}</p>
  <p v-if="dataset.source.hive" class="hint">Hive Catalog 表 {{dataset.source.hive.database}}.{{dataset.source.hive.table}} · Spark {{dataset.source.hive.sparkVersion}} · 嵌入式 Derby。管理员有界导出后登记为不可变快照，复检不连接 Metastore。凭据 SHA-256 {{dataset.source.hive.receiptSha256}}</p>
  <ol><li v-for="(part,index) in dataset.source.files" :key="part.path">{{part.name}} · {{part.rows}} 行 · {{part.size}} 字节 · <a :href="`/api/datasets/${dataset.id}/sources/${index+1}/download`" download>{{dataset.source.format==='MYSQL_SNAPSHOT'?'下载业务快照':dataset.source.format==='CONTROLLED_CSV'?'下载已登记原文':'下载原文件'}}</a><small style="display:block;overflow-wrap:anywhere">SHA-256 {{part.sha256}}</small></li></ol>
 </details>
</template>
