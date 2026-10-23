# 开发日志：数据集历史与版本追踪（2026-10-20）

从f3fc4fe干净工作树开始。实际检查Pages/PagingApi、数据集version/derivedVersions和App.vue，确认已有全局分页与直接版本关系，缺少数据集任务入口。复用原报告比较语义，未新造质量评分。

## 文件改动

- backend/src/main/java/cn/sparkdq/Pages.java：新增datasetTasks只读分页；输入dataset_id或治理output_dataset_id关联，状态条件整体作用，存在性/参数验证、稳定排序、单快照计数和列表。
- backend/src/main/java/cn/sparkdq/PagingApi.java：新增GET /api/datasets/{id}/tasks，原接口不变。
- backend/src/test/java/cn/sparkdq/DatasetHistoryTest.java：4项验证参数拒绝/不存在404、输入输出关联分页、状态与绑定参数。
- frontend/src/DatasetHistory.vue：独立数据集工作台历史、父子版本和生成/来源任务导航、报告比较、空/失败/重试及自动刷新；App.vue接入并迁移原简单版本按钮；style.css配套卡片/比较样式。
- frontend/src/dataset-history.mjs与frontend/tests/dataset-history.test.mjs：请求所有权及角色/比较状态逻辑，6项测试保护迟到响应/卸载/错误来源/负变化。
- docs/dataset-history.md、当前完整进度与本文；README、开发记录和路线更新入口。

## 验证与限制

147 Java（111+36）、14 Node、前端build通过；真实11HTTP：4成功/7预期排队取消。原始/输出/无关历史9/2/1，页5+4、状态筛选和未知/无效请求通过；主恢复点/文件/镜像不变。

实际浏览器验证原版分页、来源不匹配提示、治理质量对比、输出版本角色、空筛选及恢复；截图和API证据保存。没有数据库迁移、Spark算法或主预算更改。旧候选正常停止，所有卷保留。运维代码不变，未重复运行规模/长时间实验；Python仅本地验收脚本，业务链路仍Java/Spark。

本轮是直接数据集身份的关联历史与直接父子导航，不声称递归血缘/任意比较或家族列表全量分页。完整范围、入口和剩余工作见当前完整进度。
