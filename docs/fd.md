# B2：配置式函数依赖 FD 检测

2026-10-21；继B1 326f8d0后的独立功能批次。无数据库迁移，原主部署/旧卷未使用。

## 定义与数值口径

`{"type":"FD","columns":["x1","x2"],"params":{"dependentColumns":["y1","y2"]},"definitionVersion":1}`。columns是决定项X，dependentColumns是依赖项Y；各1～16列，字段唯一、X/Y互不重叠，且来自目标数据集。显式配置验证，不做自动发现。

精确原始文本比较，不trim、不转大小写、不拼接元组。任一参与字段NULL/空串/普通纯空格，该行跳过；tab不是普通空格。skippedCount为去重后的跳过行数，skippedNullCount/Empty/Whitespace是各原因行数，可重叠，不能相加。没有有效行时NO_VALID_ROWS，不作全表合格结论。

Spark先筛有效行，按X分组countDistinct(struct(Y))，不同Y元组>1则为冲突组；left_semi保留冲突组所有原始参与行及其重复，包括多数派。没有全表collect。checkedGroups是有效X组；conflictGroups/Rows分别是冲突组和参与行。兼容invalidCount=conflictRows供旧比较/图表使用，不把多余行或少数派计数冒称参与行。

不自动修复或挑选错误来源。模板、参数快照、原规则/新数据复检、HTML/JSON与页面组合X/Y控件贯通。执行器另外记录每规则algorithmMs、anomalyWriteMs；算法时间包含计算指标的Spark动作，写出仍可能重算惰性计划，因此不是纯CPU内核时间。

## 实际验收

165项Java通过（B1的161加4项FD参数/契约测试），17项Node通过；镜像构建成功。隔离HTTP四项：模板检测、修改模板后原规则复检、文本边界新数据复检、全跳过CSV复检。七条规则含六种FD组合和IQR；全部指标与独立tuple/set或Decimal oracle、完整异常行多重集一致。

审查侧独立11行反例实际得到9有效、2跳过、5组、2冲突组、5冲突行r1/r2/r3/r8/r9；包含多数少数派、重复行、X及Y分隔符碰撞。扩展用例覆盖NULL/空串/普通空格/tab、中文、大小写/前后空格保持原文、SQL关键字select和Case/case列名。全跳过时全部FD返回NO_VALID_ROWS，IQR返回INSUFFICIENT_DATA。

实际浏览器手动配置X=[x1,x2]、Y=[y1,y2]并提交成功，指标与独立给定反例一致，原CSV前后下载SHA相等。全部5个成功任务在同容器重启后报告JSON、规则/参数/快照、来源与原下载保持；模板修订不改变历史。

证据`.runtime/b2-fd-http-b`（COMPLETE、summary、各任务、input/edge CSV、下载、HTML、browser、persistence）；复现`tests/fd_acceptance.py --base <隔离API> --output <全新目录>`，独立样例tests/algorithm-independent-cases.json。首次不完整`.runtime/b2-fd-http-a`保留，不混入完整通过统计。

## 真实差异及修正

首轮文本边界指标差异定位为验收CSV生成器：未加引号空字段被Spark读作NULL，oracle按空字符串计数。改为QUOTE_ALL，完整保留空串与NULL标记的契约，再在新目录重跑四任务通过。另补齐oracle无有效行状态，不将NO_VALID_ROWS与EVALUATED混用。未因夹具问题改业务算法迁就预期。

本批没有性能/真实业务准确率结论。B3独立效果与有界性能实验在本批通过提交后进行；MySQL业务源/Hive仅列差距，不在本批扩源。

重启验收首次使用旧随机端口而触发健康等待超时；改为重启后重新查询Docker实际端口，再次同容器重启并完整核对通过。旧端口等待失败不冒称成功。
