# 任务取消与列表分页（2026-10-19）

本阶段完善单实例本地运行可靠性，保留此前检测、规则模板、治理版本、复检和报告流程。任务取消不删除文件或数据库历史记录；分页使页面能查询超过旧接口 200 条窗口的历史。

## 任务取消

POST /api/tasks/{id}/cancel，无请求体。状态约定：

| 当前状态 | 行为 |
| --- | --- |
| QUEUED | 直接 CANCELLED，移出等待队列并释放位置，运行耗时为 0 |
| RUNNING | 先持久化 CANCELLING，终止该任务的计算进程，确认退出后写 CANCELLED |
| CANCELLING / CANCELLED | 幂等返回当前详情 |
| SUCCEEDED / FAILED | 返回 409，历史终态保持不变 |
| 不存在 | 返回 404 |

取消与成功登记使用同一任务行锁仲裁。取消先获得锁则不登记 SQL 结果、治理输出版本或审计；成功登记先获得锁并提交则返回取消冲突。已经写出的临时日志/中间文件保留供排查，不作为成功结果提供。不能将“已接受取消”当成“进程已经停止”。

每个任务独立持有取消标记与计算进程，避免延迟到达的取消请求终止下一个任务。排队发布和取消移除协调，避免任务刚登记但还未入队时取消留下占位。停止过程有界；若进程不能确认退出，保持 CANCELLING，不伪报 CANCELLED。

CANCELLED 可按冻结规则复检，创建独立任务；治理仍要求成功来源。取消记录可通过 GET /api/tasks/{id}/report 下载，包含状态、原因和快照，不提供成功统计或前后质量对比。异常预览仍仅提供成功任务的已登记结果。

重启时 CANCELLING 恢复为 CANCELLED 并记录清理原因，原 QUEUED/RUNNING 继续按原逻辑标记 FAILED/重启中断。该恢复依赖当前单后端容器边界：重启前原容器计算进程随容器停止。未扩展为远程 Spark 作业或多后端协调。

## 分页接口

旧 GET /api/tasks、GET /api/datasets 仍返回最多 200 条数组，兼容原有调用。新接口：

- GET /api/tasks/page?page=1&pageSize=20&status=ALL
- GET /api/datasets/page?page=1&pageSize=20&q=名称关键词

统一返回 items、total、page、pageSize、totalPages。页号从 1 开始；pageSize 1～100、默认 20，page 最大为 2147483647，偏移使用 long 避免溢出。只接收十进制正整数，不接收负数、小数、正号或空参数。未提供参数使用默认值；非法值返回 400。无匹配时 totalPages=0，越界页 items=[]，不修改请求页码。

任务 status 严格大写，支持 ALL、QUEUED、RUNNING、CANCELLING、CANCELLED、SUCCEEDED、FAILED。名称搜索去首尾空白、最多 200 个 Java UTF-16 字符单元，按字面子串匹配，百分号和下划线不作为通配符，大小写遵循 MySQL 列排序规则。查询使用绑定参数。

每页按 created_at DESC、id DESC 稳定排序；总数和该页记录在一个只读 REPEATABLE_READ 事务中取得一致视图。跨页请求不是冻结游标，在持续新增或状态变化时 offset 页仍可能移动；需要严格历史导出时应另增快照/游标机制。

## 页面

任务列表支持每页 20 条、前后页、总数与状态筛选。数据集支持名称搜索和前后页，翻页或搜索保留当前已选数据集并显示其名称，即使它不在该页。新建/复检/治理任务回到全部状态首页。

任务页、数据集页、数据选择和详情分别隔离异步请求，防止较早轮询响应覆盖新选择。运行/排队任务可取消；停止中清楚显示进程仍待退出；已取消显示原因、复检入口和取消记录下载。

## 验收

构建并启动本地容器后，从根目录执行：

    timeout 1200s python3 tests/cancellation_paging.py --verify-engine-log
    timeout 120s python3 tests/cancellation_persist.py
    timeout 240s python3 tests/e2e.py
    timeout 360s python3 tests/governance.py
    timeout 360s python3 tests/rule_templates.py
    timeout 480s python3 tests/governance_transforms.py
    timeout 240s python3 tests/rerun.py

--verify-engine-log 需要本项目 Docker 只读探测权限，检查真实 Spark 日志及该任务 Java /proc 命令行，取消后确认进程不存在。8 槽队列验收只创建新合成数据/任务，finally 只清理本进程创建的任务。不要在同时运行别的任务/自动验收时执行容量测试。

在无活动任务的维护窗口，生成全部夹具后执行 timeout 600s python3 tests/cancellation_recovery.py。脚本拒绝有 QUEUED/RUNNING/CANCELLING 的环境，先验证旧数据，再停止本项目后端，仅将本轮夹具中专用的已取消 recoveryId 模拟为 CANCELLING（UUID、源数据集、合成名称、原状态均有检查，SQL 必须恰好影响 1 行），保留数据卷停止/启动本项目容器，再验证恢复及全部持久化。密码只在 MySQL 容器环境中读取，不输出或写入命令行。该脚本是受保护的故障验收，不是正常业务 API。

2026-10-19 实际结果：

- Maven package 与 Docker 后端/前端构建成功；31 项 Java 测试通过：原 16、分页 5、取消与竞态 10。首轮 2 个分页测试空 varargs stub 不匹配已修正；保留失败和最终成功日志，不把首轮写成成功。
- 新取消/分页真实验收通过：队列满 8 槽返回 429 且不登记任务；取消排队治理立即释放位置；实际运行中的 Spark JVM 已取证并在取消后消失；后续任务成功；取消幂等、终态冲突、无半份 SQL 结果/治理版本、原始 CSV 和历史统计保留；分页/筛选/字面搜索/越界/旧数组兼容通过。
- 上列 5 个既有流程验收全部通过；graceful_stop.py 也通过，确认新停止逻辑仍保存明确的中断原因及耗时。
- 真实停止/启动前后，cancellation_persist.py、governance_persist.py、rules_persist.py、governance_transforms_persist.py、snapshots_persist.py 均通过；专用 CANCELLING 注入恢复为 CANCELLED，并保留明确的服务重启清理原因。
- 浏览器验证了搜索、第二页任务、状态筛选，以及搜索无匹配时保留已选数据集。页面提交并取消任务 992539f9-951c-4c51-908c-a27661cb03ac，耗时 3833 ms，状态 CANCELLED，没有 SQL 结果；显示取消原因、复检及取消记录下载。另完成 8 项静态异步响应/错误分支模拟，不将其等同于完整组件测试。
- 独立审查修复了队列发布间隙和停止超时丢失 Job 引用两个问题。超过首次 10 秒等待仍未退出的进程引用会保留，由单 daemon 每 1 秒非阻塞重试；确认退出并写库后才移除，不误杀后续任务。服务重启期间空/非 JSON 响应改为可读提示。

真实运行取消任务 9dd83841-8bdd-4ca4-bf6c-4ca392336bc7；排队治理取消任务 c12e4226-d94a-4a60-a559-29e09779e645；随后成功任务 d307132c-a830-44be-982d-120a532d402c；取消后复检 a6f892de-3b4d-40d1-9f6b-a2bfddae8e39；专用恢复 ID 94e93b83-dcb5-4ddc-9eb3-062d3dd4d806。全部为本轮新建的合成数据。

此轮未运行 lifecycle.py 的 SIGKILL 场景、edgecases.py、operational.py、csv_compat.py 或 precise_bounds.py；不将此前结果充作本轮结果。

## 范围与下一步

本阶段没有修改用户文件、删除历史、推送远程或引入依赖；status 现有 VARCHAR(16) 可保存新增状态，不需要重写旧表。数据库迁移、只读文件对账、备份恢复、WSL 长期启动和分布式状态协调仍待完成。

数据库不可用时，普通任务 FAILED 写入本身仍可能失败，需重启恢复遗留 RUNNING/QUEUED；本阶段清理重试主要保证取消状态和进程引用，不宣称已完成数据库故障协调。后续应在隔离环境补充数据库断连/恢复实验。
