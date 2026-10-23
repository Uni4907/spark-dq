# 开发日志：可阅读质量报告（2026-10-20）

从dcb234b干净工作树开始，检查真实App.vue/Reports/TaskApi，确认只有JSON导出和工作台统计。选择补齐可直接阅读/打印的报告，未新增与闭环无关组件。

## 文件变化

- backend/src/main/java/cn/sparkdq/ReportDocuments.java：服务端独立HTML文档，使用现有Reports结果；规则统计、前后变化、治理策略和操作审计、版本SHA与输入快照；失败/取消抑制成功指标，历史缺失不造数，统一HTML转义、A4打印样式。
- backend/src/main/java/cn/sparkdq/TaskApi.java：增加report.html内联/附件两种呈现，UTF-8/no-store/nosniff/CSP；原JSON接口不变。
- frontend/src/ReportActions.vue：新报告操作面板；App.vue接入全部终态，移除重复JSON链接；style.css增加配套样式。
- backend/src/test/java/cn/sparkdq/ReportDocumentsTest.java：8项功能/边界/HTTP测试。
- docs/report-documents.md、docs/progress-delivery-report-documents-2026-10-20.md、本文及README/开发记录/路线入口：用法、边界和完整交付。

## 实际验证

Maven package成功，143项Java通过；前端build成功，8项Node及202项运维测试通过。5个真实HTTP任务3成功/1预期治理失败/1排队取消，检测→治理→复检完整数据预期核对；HTML与下载字节相同、中文转义与409/404通过。浏览器实际点击入口、检查独立报告/对比/审计和失败面板，截图保存。

新镜像仅使用report-document-20261020候选标签；独立新lab保留原主库/文件/镜像，默认300秒预算。未发现需迁移数据库或修改Spark算法的问题。原始验证文件/示例报告与源码一并交付，未打包.env或凭证。不把CSS打印支持描述为已自动生成PDF或已做实体打印机验收。
