# SparkDQ 无人值守开发日志

日期：2026-10-18（Asia/Shanghai）

## 真实项目盘点

- 实际仓库：WSL2 `SparkDQ-Ubuntu` 的 `/home/sparkdq/projects/spark-dq`；开始时 Git 工作区干净，HEAD `e6567ea`。未找到适用的 AGENTS.md。
- 阅读了 README、后端 API/调度器/数据集校验、SQL、Spark 引擎、Vue 页面、Compose 和既有测试。CSV 上传→独立 Spark JVM 检测→MySQL 保存→前端查询已实现；现有超时 300 秒、串行队列、失败原因和启动恢复逻辑也已实现。
- 开始时服务未运行；原始基线尝试先遇代理 502，绕过代理后又发现服务随 WSL 会话结束而停止，故没有把首次基线尝试计为通过。升级后完整执行并通过原 MVP 测试。

## 本次完成的增量

1. 新增 `task_snapshots` 表，保存任务输入 SHA-256、规则定义、口径版本 `csv-v1`、来源任务与时间。与任务、规则在同一事务入库；仅新增表，不修改或回填旧记录。
2. `POST /api/tasks/{id}/rerun`：省略 body 或传 `{}` 复检原数据；传 `{"datasetId":"目标数据集ID"}` 对已注册的新数据沿用原规则复检。仅接受 SUCCEEDED/FAILED 来源任务；正在执行返回 409，字段不兼容返回 400，不存在返回 404；校验失败不创建任务。
3. 新任务、规则、结果使用独立 ID；原任务与结果不变。新任务优先从 JSON 快照取规则；旧 MVP 任务从持久化 rules 表取规则，不虚构旧输入快照。详情返回 `snapshot`，历史任务为 null。
4. 实际执行前重新计算输入 SHA-256，不一致则持久化 FAILED 和明确原因，不启动 Spark。提交到已停止调度器返回 503；事务完成后发生调度拒绝也会记录失败，避免留下无法执行的 QUEUED 任务。
5. 页面新增原数据复检、左侧所选数据复检和来源任务入口，展示指纹与口径。复检忽略左侧规则勾选，使用来源快照。治理后的 CSV 可以单独上传后复检。
6. 修正本地 HTTP 测试受继承代理影响的问题，只对本地测试禁用代理，不改系统代理设置。

## 改动文件

- `backend/src/main/java/cn/sparkdq/Tasks.java`：快照、复检、输入校验、调度拒绝处理。
- `backend/src/main/java/cn/sparkdq/TaskApi.java`：复检接口。
- `backend/src/main/resources/schema.sql`：追加快照表。
- `frontend/src/App.vue`：复检入口与快照展示。
- `backend/src/test/java/cn/sparkdq/TasksTest.java`：输入指纹失败落库、停止调度器拒绝的单元测试。
- `tests/e2e.py`：回环地址直接访问。
- `tests/rerun.py`：原数据、新数据、旧任务、字段不兼容、来源与历史保留的真实 Spark 验收。
- `tests/snapshots_persist.py`：重启后对快照、指标、来源、异常预览作只读校验。
- `README.md`、`docs/development.md`、本日志：使用与验收记录。

## 实际测试结果

| 测试 | 本次结果 |
| --- | --- |
| 隔离容器 Maven package、后端/前端镜像构建 | 成功；Vite build 成功，仍提示单块大于 500 kB |
| Maven test | 2 tests，0 failures，0 errors，0 skipped |
| `tests/e2e.py` | 通过：非法上传、7 行已知异常、复合/空键、清洁数据、结果 ID 与异常预览 |
| `tests/rerun.py` | 通过：原数据同指标、新数据零异常、不同 ID、来源关系、400/404/409、旧任务兼容、历史不变 |
| `tests/edgecases.py` | 通过：大小写字段、count 业务键、空白物理行、BOM/带引号换行 |
| `tests/operational.py` | 通过：环境文件生成与保留、超大上传 413、10000 行合成样本 |
| 10000 行检测 | 5473 ms；100 个空 email，5000 个重复组，10000 个涉及行，5000 个多余行 |
| 真实浏览器复检 | 页面点击复检后任务 `7eb24380-16b1-48b7-ad0b-455450977461` SUCCEEDED，2 行、5800 ms、所有异常为 0；来源入口回到原任务，截图已保存 |
| 后端重启持久化 | 重启前及重启后只读校验均通过；重启前确认没有 QUEUED/RUNNING 任务 |

所有计算验收使用真实 MySQL 和独立 Spark 3.5.8 JVM。Java 单元测试中的 JDBC 为 mock，只证明失败处理分支，不冒充真实数据库测试。未执行会强杀/重建服务的既有 lifecycle.py、graceful_stop.py，也未重新等待 300 秒制造超时，不能声称本次验证了这些场景。

## 可复现步骤

在 WSL 项目目录、具备现有 Docker 权限的会话中：

```bash
timeout 480s bash infrastructure/dev.sh build
timeout 120s bash infrastructure/dev.sh up
# 等待健康接口可用；本机 curl 使用 --noproxy '*'
timeout 240s python3 tests/e2e.py
timeout 240s python3 tests/rerun.py
timeout 240s python3 tests/edgecases.py
timeout 150s python3 tests/operational.py
# tests/rerun.py 创建 .runtime/rerun.json 后，在后端重启前后各执行一次：
timeout 20s python3 tests/snapshots_persist.py
```

构建会运行后端单元测试。Python 验收仅新增合成数据与任务，会保留这些记录；不删除历史数据。重启前必须确认没有在执行的任务；不要在用户有运行任务时执行重启测试。

## 限制与后续工作

- 本次完成复检基础，不是完整治理系统：尚无自动清洗、新数据版本树、前后指标比较报告、规则模板编辑器或分布式集群验证。治理样本是测试生成的干净 CSV，并非系统自动修复产生。
- `csv-v1` 固定当前统计定义；未来改变规则语义时需维护版本兼容。数据集只读约定不等于抵御管理员并发篡改；当前指纹检查是执行前校验。
- 当前仍是单实例、串行任务、20 MiB CSV 上限；没有测量 TB 级数据能力。队列拒绝竞态处理的真实关闭时序未专门压力测试。
- 新表通过现有启动 SQL 幂等创建，正式迁移工具仍未引入。旧任务没有伪造的历史输入快照。
- 当前机器非交互 WSL 会话退出后，观察到项目容器被停止；验收期间以最长 15 分钟的前台会话临时保持运行。没有改系统自启动、WSL 配置或 Docker 权限。需要长期运行时，应保持现有 WSL 终端会话运行；这属于环境运行条件，代码与持久化数据已保留。
- 首次 Java 单测命令挂载目录错误，Maven 未找到 POM；修正为明确 Linux 路径后测试成功。最初串行验收命令被跨 shell 引号破坏，改用有时限脚本后全部执行成功。
- 未读取/输出 .env 内容，未改 Windows 旧 Java/Spark/MySQL，未删除数据卷、未重置仓库、未推送或发布。

## 用户回来后

无需补做开发步骤或认证。可在工作台选择 `rerun-governed.csv` 对应的已完成任务，查看快照、零异常结果与来源；如服务已随 WSL 会话关闭，按 README 的既有启动方式恢复即可。下一阶段可在此基础上实现人工选择治理策略、生成独立数据版本以及前后指标对比报告。

## 本次真实任务证据

- source: `44e7357a-ec01-4e42-b94b-dafe0cd5f4a6`，状态 SUCCEEDED，3 行，5402 ms。
- same: `d00fe15e-3f2e-4422-b2e1-b338845a3d85`，状态 SUCCEEDED，3 行，5078 ms。
- governed: `1a28fcef-c5c2-4144-835d-252f7ffac7ed`，状态 SUCCEEDED，2 行，5083 ms。
