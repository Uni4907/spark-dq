# 缺失值填充与文本标准化（2026-10-19）

本阶段在已完成的规则模板和治理版本闭环上，增加按字段配置的常量填充、文本标准化及逐操作审计。治理仍由独立 Spark JVM 执行，生成独立 CSV，实际重新读取保存的文件并按来源规则复检；原文件、原任务和此前版本保留。

## 页面与接口

成功的 csv-v2 任务详情展开“生成新的治理版本并自动复检”，添加标准化字段/操作或填充字段/常量，再按需选择移除仍缺失记录和业务键去重。字段来自有效输入：来源是治理任务时，使用其输出版本。

POST /api/tasks/{sourceId}/govern 示例：

    {
      "policyVersion": 2,
      "normalizations": {"code": "TRIM_UPPER"},
      "fills": {"name": "未填写"},
      "dropMissing": false,
      "deduplicate": false
    }

可用标准化操作：TRIM、UPPER、LOWER、TRIM_UPPER、TRIM_LOWER。每个字段在一种操作映射中只配置一次，可对同一字段先标准化再填充。未选字段不变。填充值必须为字符串、非空白、不含 NUL、不等于系统保留空值标记，且最多 1000 个 Java UTF-16 字符单元。只含旧版两个布尔参数的请求仍可用，新请求规范化后冻结为 policyVersion=2、包含完整映射。

固定顺序：文本标准化 → 填充 NULL/空字符串/纯普通空格 → 移除仍命中来源全部 MISSING 规则的记录 → 按来源 DUPLICATE 规则去重 → 写新 CSV → 从文件复检。填充常量不会在本次处理再次标准化；非缺失值不替换。TRIM 与缺失判定采用 Spark 普通空格 U+0020 口径，不扩展为所有 Unicode 空白，制表符保持原值。大小写转换使用 Spark 字符串函数。

标准化业务键后，原来不同的键可能相同，去重使用处理后的键。去重保留规则仍是全部字段字典序、NULL 在前，不推断最新或最优业务记录。若没有 MISSING/DUPLICATE 规则，相应移除/去重策略在入队前拒绝；填充和标准化不要求目标字段已有检测规则。

未知顶层参数、未知字段/操作、非对象配置、非字符串配置值、非法常量、未知版本、空策略均在入队前返回 400。新操作可能产生保留空值标记，例如去空格或转大写后形成反斜杠 N；Spark 在写 CSV 前拒绝此任务并保存明确失败原因，不登记输出版本、审计或半份 SQL 结果，避免字符串被重读成 NULL。

## 冻结策略与操作审计

governance.policy 保存规范化的策略快照；governance.audit 保存 Spark 证据：policyVersion、inputRows、outputRows、removedRows、droppedMissingRows、deduplicatedRows、transformations。每条 transformation 包含 column、operation、changedRows，先标准化后填充，各阶段按字段名排序。

changedRows 通过 Spark 空值安全比较前后真实值计算，每阶段用一次聚合同时统计所选字段。零变化仍保存 0，生成独立版本。不同操作可能改变同一行，不能把 changedRows 相加当成唯一受影响行数；操作后被移除的记录仍计入该阶段改变行数。复检结果和报告如实保留类型/范围/正则异常，不承诺自动纠正未配置的问题。

新增 governance_audits 表以非破坏性 CREATE TABLE IF NOT EXISTS 建立，不重写历史行。版本、操作审计、结果及成功状态在同一数据库事务登记。历史任务没有逐操作证据时返回 audit:null，保留历史策略原文，不追补推测计数。报告内 task.governance 同时提供策略及审计。

## 验收与可复现步骤

所有数据均为新建合成数据。现有数据及结果不删除、不覆盖。先构建并启动本地容器，然后从仓库根目录运行：

    timeout 480s python3 tests/governance_transforms.py
    timeout 240s python3 tests/e2e.py
    timeout 360s python3 tests/governance.py
    timeout 360s python3 tests/rule_templates.py
    timeout 240s python3 tests/rerun.py

新验收覆盖 30 种入队前拒绝、5 种标准化、选择性填充、同字段顺序、标准化业务键合并、两种零变化操作、Unicode/逗号/引号/反斜杠/CRLF/多行 CSV 保真、原始 SHA-256/结果不变、独立版本/规则/结果 ID、实际文件独立复检及七规则对比报告。两种保留标记碰撞验证失败状态及没有版本/SQL 结果泄漏。组合样本实际为 7 → 5 行。

fixtures 位于 Git 忽略的 .runtime/governance_transforms.json。生成夹具后，重启前后各运行 timeout 90s python3 tests/governance_transforms_persist.py。重启前必须确认没有 QUEUED/RUNNING 任务；仅使用保留数据卷的停止/启动，不移除卷。

2026-10-19 实际结果：Maven package 与两项 Docker 镜像构建成功，16 项 Java 单元测试全部通过（原 11 项 + 策略 5 项）；前端 Vite build 成功，保留既有大 chunk 提示。上列 5 个真实 HTTP/Spark 验收脚本全部通过。新持久化脚本在停止/启动 MySQL、后端和前端前后均通过：8 个任务细节、5 个输出版本、6 份七规则报告完全一致；旧 governance_persist.py、rules_persist.py、snapshots_persist.py 同时通过。历史治理任务确认两布尔策略原文不变且 audit:null。

组合治理任务 c0f92654-5428-454b-bd43-b73785c4a8b3，输出数据集 27487f70-8a4a-47cb-b109-31b9287806a7，7 → 5 行，耗时 7563 ms；移除仍缺失记录 1 行、去重移除 1 行。name/age 填充各影响 3 行。复检保留 age 类型异常 1、范围异常 2、email 格式异常 1，未掩盖未修复问题。独立复检任务 d4a46c9b-421b-41d8-82be-27efcd74109d 指标一致。

真实浏览器配置并提交任务 efcf435a-7567-447d-9241-a3b5522913f3，7 行保留，key 标准化改变 6 行，name 填充改变 3 行；name 空值 3 → 0，标准化导致重复涉及行 0 → 4（未勾选去重），报告显示减少量 -4。页面成功展示策略、操作审计、下载与指标对比，并导出截图。没有把标准化等同于所有质量指标必然改善。

当前阶段没有再次执行 edgecases.py、operational.py、csv_compat.py 或故障注入 lifecycle/graceful_stop；其历史结果不算作本次新验收。独立代码审查未发现新增阻塞，git diff --check 通过。

## 变更与后续

后端新增 GovernancePolicies 和单元测试，Tasks 增加配置校验/快照/审计查询，schema 增加审计表；Engine 用 DataFrame 执行并计数；前端新增 GovernancePanel 并在详情展示策略/审计；新增真实 Spark 验收及重启持久化脚本。没有增加依赖。

尚未提供均值/中位数填充、映射字典、按时间/业务优先级保留、任务取消和分页。输出仍受 20 MiB、100 列、单元格 65536 字符及 300 秒子进程限制；填充扩大文件超过上限会安全失败，不修改源数据。文件写入和数据库事务不构成跨资源事务，数据库登记失败时可能保留孤立文件，后续应先做只读对账。WSL 长期启动、数据库迁移、备份恢复和分布式基准仍需单独完成，不宣称已解决。
