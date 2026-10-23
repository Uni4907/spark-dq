# 业务去重保留优先级开发日志

时间：2026-10-19T15:12:03+08:00。真实项目 /home/sparkdq/projects/spark-dq，起点main/04b7b5e，初始工作区干净。没有推送远程、对外发布、删除用户数据、清空卷或用旧备份覆盖原卷。

## 本阶段能力

在已有独立版本和复检闭环中加入v4保留顺序。最多5级，按用户列表顺序决定优先级；支持文本、decimal(38,10)精确数值、严格日期YYYY-MM-DD，各级可选升降序和空值最前/最后。优先级全部并列时沿用全字段原文本字典序、NULL在前。没有优先级的v2/v3行为与JSON形状保持兼容，空新字段省略。

排序读取标准化/映射/填充/移除后的数据，只建立Spark排序表达式，不改写原字段文本。非空但不合法的数值/日期使本次治理FAILED，无新版本、部分结果或虚构审计；可先显式治理修复。NULL/空字符串/仅普通空格统一作为排序缺失值。审计冻结实际顺序、并列规则、去重候选行数和重复业务键组数，并进入页面和JSON报告。

## 具体文件

- backend/src/main/java/cn/sparkdq/Tasks.java：SortSpec严格反序列化、治理请求新字段和旧6/5/2参数构造兼容。
- GovernancePolicies.java：版本2/3/4选择、字段/类型/方向/空值位置/5级限制、不可变列表和必须启用去重的前置检查。
- spark-engine/src/main/java/cn/sparkdq/engine/Engine.java：Spark typed排序、无效值失败保护、稳定并列规则和去重审计；重复组内部别名避免合法业务字段count冲突。
- frontend/src/GovernancePanel.vue、App.vue：顺序编辑/上移下移/移除、重复字段禁止提交、v4提交、完整策略及审计展示。
- backend/src/test/java/cn/sparkdq/RetentionOrderTest.java：新增7项测试。
- tests/retention_order.py：新综合真实验收和只读持久化核对；governance_transforms.py/value_mappings.py仅将未知版本探针由4改为5，保留既有精确快照断言。
- README、roadmap、retention-order.md、value-mappings.md、development.md、本日志及当前进度报告：功能口径、复现与真实边界。

## 构建与真实验收

Maven package成功，55项Java测试（原48+新增7）0失败/错误/跳过；48项现有Python工具单元测试通过；后端/Spark引擎与Vue/Vite候选镜像构建成功，无新增依赖或数据库迁移。

从上一阶段最新受控备份恢复新项目/新卷。隔离副本21项任务快照综合验收通过：严格请求拒绝且不入队、旧字典序、文本9大于10与数值10大于9的差异、9007199254740992.0000000001/.0000000002精度、日期、多级顺序改变实际保留结果、四种方向/空值组合、NULL及复合业务键、稳定并列、变换/移除先于去重、幂等、实际文件复检与报告。非法数值/日期安全失败，映射修正后可成功；空输出版本保留。

隔离新容器启动前预置同owner维护标识，启动仍拒写503，原业务行/全部文件/.env一致；解除保护后核对旧v4夹具完整保留。重新增强优先级先后与复合键用例后完整通过。最终隔离夹具 /home/sparkdq/projects/spark-dq/.runtime/retention-b3dc6fae346c.json。

真实浏览器在隔离环境勾选去重、添加数值/日期级别、验证重复字段阻止提交、上移日期优先并提交。任务cef1beef-81a3-44c5-8ac7-c2b58f4c4bd6成功，13→5行、重复业务键5组、移除8行；页面有真实指标对比和审计。截图已留存。

主环境先维护关写、停机一致备份、再换代码镜像，新容器在保护下核对全部旧业务行、文件与.env不变后恢复可写。部署记录 /home/sparkdq/projects/spark-dq/.runtime/priority-deploy-4e65f04b3755/receipt.json；部署前恢复点 /home/sparkdq/projects/spark-dq/.runtime/priority-deploy-4e65f04b3755/backup。

主环境本轮重新运行MVP e2e、v2填充/标准化完整验收、v3映射完整验收、v4保留顺序21项任务验收。v4主任务 4de5ce0f-cf8d-4a66-8c43-c11e6bafd645，组合治理 656ef62a-047f-49ef-88c4-cb3da2b7ba94，零行版本 7f85a4cc-72fa-42a7-8263-2205d77aedf3，复检 ddb55877-34a8-4c92-bf78-6a012a373883。

13行样本按单业务键保留5行、移除8行；复合业务键保留7行，重复组4组，证明没有跨键丢失。数值优先保留10/2026-01-02，日期优先保留9/2026-01-03，明确验证列表顺序影响选择，而不是只检查行数。

随后受控备份并真实停止/启动同一后端，自动恢复可写。v2/v3/v4在重启前后均通过快照/策略/审计/CSV摘要/结果/报告核对；历史治理、模板和带恢复原因的取消记录全部通过。重启备份 /home/sparkdq/projects/spark-dq/.runtime/priority-persist-b6b63cf9c1d2/managed-backup/backup，主夹具 /home/sparkdq/projects/spark-dq/.runtime/retention-df8a23e58d4b.json。旧记录未改，新增测试数据均保留。

当前 213 数据集、269 任务，状态分布 [{"count": 10, "status": "CANCELLED"}, {"count": 20, "status": "FAILED"}, {"count": 239, "status": "SUCCEEDED"}]；无活动任务、无快照指纹不符、无成功规则缺结果。13张表含12业务表和迁移历史表，schema仍2，维护已解除，前后端代理健康UP。

## 实际问题与修正

第一次Maven调用的挂载变量在跨Windows/WSL命令传递中失效，容器内未见pom；改为明确绝对路径并通过结构化子进程参数重建，55项测试和镜像构建均成功。没有把第一次“无pom”的运行算作通过。

增强复合键脚本查找NULL标记时多转义了一层，出现StopIteration；修正测试文字后完整重跑21任务验收。失败日志保留 .runtime/priority-isolated-first-failure.log。实际Spark保留结果没有该问题。

初查全目录时私有运行目录不可读，改为跳过.runtime与.git查找项目约束，未修改目录权限。新发现原后台会话已失效但服务健康；用现有有限会话入口恢复2小时开发运行，没有修改开机/定时设置。交付时会话状态 running，期限 2026-10-19T16:40:49+08:00，用户时区为Asia/Shanghai。

## 交付与后续

隔离测试容器已停止，卷、成功/失败任务及备份保留；只有主三服务运行。导出跟踪源码、文档、合成输入/输出、JSON报告、测试摘要与截图，不导出.env、维护密钥或业务备份SQL/tar。没有自动清理、远程推送或公开发布。

本阶段完成业务保留顺序，下一重点转向系统性规模/分区/规则组合基准实验，再推进集群提交与共享存储/状态协调以及论文实验和答辩材料。仍是单机local[2]，没有把本阶段小样本正确性当作大规模性能证明。
