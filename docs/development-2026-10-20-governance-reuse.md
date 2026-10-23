# 开发日志：历史治理策略复用（2026-10-20）

从7aee429干净工作树开始，检查RuleTemplates/GovernancePolicies/GovernancePanel，确认规则已有模板管理而治理策略只随任务保存、没有复用表单入口。选择复用成功治理的实际快照，避免新表牵动迁移/备份契约。

## 改动文件

- backend/src/main/java/cn/sparkdq/GovernanceReuse.java：只读兼容检查和独立规范化副本，目标有效版本/成功状态/口径/字段及规则资格校验。
- backend/src/main/java/cn/sparkdq/GovernanceReuseApi.java、Pages.java：成功治理策略分页与字面搜索、准备接口，原执行/快照接口保持。
- frontend/src/GovernanceReusePanel.vue：分页搜索、来源报告、明确替换操作和错误提示；GovernancePanel.vue接入、撤销及当前规则说明。
- frontend/src/governance-policy.mjs及测试：独立深层表单副本，保留字面值/映射/优先级，3项Node测试。
- backend/src/test/java/cn/sparkdq/GovernanceReuseTest.java：8项兼容/不可变/有效版本/状态/损坏记录与分页搜索测试。
- docs/governance-reuse.md、当前完整进度、本文及README/开发记录/路线：用法、真实验证与范围。

## 验证

155 Java（119+36）、17 Node和前端build通过。真实5任务全部成功，浏览器带入/撤销/再次带入/修改/提交，1000→500；新配置name=REUSED_NAME，来源UNKNOWN不变。完整异常/治理行/指标/审计与独立预期核对，主恢复点/文件/镜像不变。普通检测来源409、字段不兼容400、中文字面搜索与输入限额通过。

验收助手首次把中文直接拼入HTTP URL，出现UnicodeEncodeError；已补标准URL编码，并复用已成功的4个任务继续只读检查，没有重复Spark执行或改业务代码来掩盖错误。原错误日志及恢复说明保留，浏览器流程由页面真实请求完成。

只读准备不执行治理，提交仍沿既有后端校验；新任务保存最终策略。无数据库迁移/备份集合更改，不把此功能描述为独立命名策略模板或草稿库。所有候选卷保留，无远程推送/主部署替换。运维Python源码未改，未重复无关实验。
