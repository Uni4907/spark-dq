# 开发日志：local / 同机集群规模对照（2026-10-20）

以ebf819b为起点，主仓库初始干净。承接前阶段“完成同口径性能对照”的建议，新增worker级配对基准、真实资源采样与可复现图表，没有改Java业务或Vue页面，没有替换主镜像。

## 新增代码

- infrastructure/cluster_compare.py：固定种子与随机配对顺序；1万/20万行、检测/治理、两模式各两次正式测量；限制资源，逐次独立正确性、全量异常跨模式对照、双Worker实际参与证据；cgroup CPU/内存采样；输入/源码/主数据对账后才发布COMPLETE。
- infrastructure/plot_cluster_compare.py：校验COMPLETE内容摘要后绘制PNG/SVG，输出数据来源摘要，不将绘图依赖加入业务服务。
- tests/test_cluster_compare.py：12项新回归，覆盖成对计划、限额拒绝、同时刻内存求和、累计CPU重置/缺失、已退出cgroup、排除预热、缺配对拒绝、子网/路由重叠、Docker空IPAM及绘图完整性。
- docs/cluster-compare.md、本日志、完整进度、README/路线/开发总日志。

## 实际环境和控制

本机WSL2 Ubuntu24.04.5，Linux6.18.54.1，6个可见CPU、约11.68GiB内存，i7-12650H，Docker29.9.1。采用相同官方Spark3.5.8镜像SHA与同一引擎JAR。local与集群作业侧总配额均2CPU/5GiB；请求的总Java堆上限均2560MiB，但local为单JVM，cluster为Driver1024+两个Executor各768MiB。集群额外Master0.5CPU/512MiB；其运行开销计入采样。实际21个容器的NanoCpus、Memory/Swap、用户、只读根、挂载归属及停止状态均核验。

检测输入/Shuffle固定8/2、AQE关闭、locality.wait=0s，两模式统一。不能把相同聚合预算说成逐线程、GC/native或文件缓存完全相同；不能与旧local[2]/2GiB基准直接混为一组。本轮控制器约0.25秒采样，内存为同一采样各容器memory.current之和峰值，含文件缓存，不是RSS；CPU是最后有效采样累计值下界，不是精确完整进程总量。

Java的Runtime.maxMemory观测为local 2560MiB、远程Driver约989.875MiB，与Driver请求的1024MiB不完全相等。最初导出检查将二者误作精确相等而拒绝；已改为核实真实启动参数、保留观测值并区分配置与运行时口径，没有修改或重跑试验来追求相等。Executor的768MiB为配置值，未冒称已有逐Executor实际最大堆观测。未统一固定GC策略，因此GC/堆分布差异仍是条件边界。`n`n## 试次与问题处理

- A：Docker host/none网络IPAM.Config为null，清单解析失败，未启动试验计算容器。兼容空IPAM后补回归。
- B：受限UID185无法读取root私有request.json；明确将新合成请求文件归属交给UID185，保留私有权限。local无网络时为自身hostname提供回环映射，避免无关名称解析告警。
- C：数据指标正确、两个Executor均注册，但全部235个成功Task来自worker2；双Worker参与门槛拒绝该试次，没有COMPLETE，也未纳入统计。未将“注册”当作“参与”。
- D：统一两模式locality.wait=0s后完整重跑；两个Worker每次都有成功任务且失败为0，18次全部通过。该设置参考官方Spark3.5.8配置文档；观察与本地性调度一致，但未声称单一设置能解释所有调度现象。

没有修改主任务参数。每次用新.runtime目录，不覆盖失败试验；先前自有演示会话在空闲时停止并保留卷，性能测量只与主服务三个空闲容器共存。Docker默认地址池紧张，本工具检查Docker现有子网和主机路由后使用无重叠internal /28，没有删除历史网络或数据卷。结束只回收本次标签匹配且已空的网络，所有试验容器已停止。

## 最终结果

18次真实Spark运行（2预热+16正式）。7规则全部指标及治理审计/完整治理CSV与独立预期相同；每对local/cluster全量异常多重集SHA相同；每次远程两个Worker均参与。主库/文件起止一致；主镜像未变。

| 行数 | 流程 | local中位秒 | cluster中位秒 | cluster/local耗时比 |
| --- | --- | --- | --- | --- |
| 10,000 | 扩展检测 | 13.01 | 48.27 | 3.71× |
| 10,000 | 治理及复检 | 14.97 | 56.41 | 3.77× |
| 200,000 | 扩展检测 | 18.62 | 50.50 | 2.71× |
| 200,000 | 治理及复检 | 18.55 | 56.95 | 3.07× |

耗时比大于1表示本轮cluster更慢，不是加速比。每组n=2，中位数和min/max只作描述，无显著性结论。当前受控20MiB内样本、冷应用启动、单CSV读取、Driver/Worker配额分拆等条件下，未观察到集群提速。下表是Driver记录的各阶段墙钟计时，不能把它解释为远程Executor CPU耗时：

| 模式 | 20万行流程 | Spark启动秒 | 读取计数秒 | 治理及输出计数秒 | 规则/异常写出秒 |
| --- | --- | --- | --- | --- | --- |
| local | extended | 1.06 | 5.61 | 0.19 | 9.58 |
| local | governance | 0.80 | 4.44 | 7.13 | 4.35 |
| cluster | extended | 6.48 | 14.70 | 0.71 | 23.29 |
| cluster | governance | 6.78 | 14.64 | 19.28 | 11.09 |

没有用阶段计时证明唯一瓶颈；读取、治理/写出及多进程通信开销仍需更细剖析。依据这些证据，主环境保持local，后续优先在同口径下对照分片CSV/Parquet并检查实际读取计划，再决定扩容。

## 测试与证据

Python运维全量148项通过（本阶段新增12项）；3个新/变更Python源文件语法检查通过。Java/Vue业务未变，本阶段未重跑前阶段130项Java及8项Node，不把历史测试冒称成本轮新测试。

验收目录：.runtime/cluster-compare-20261020-d。plan/environment/host-observation、逐试次request/driver.log/result/executor-evidence/resource-samples/measurement均保留；summary.json、measurements.json和COMPLETE摘要校验通过。起止主业务恢复点仍为1c2caca9b8dff53db7627c96c051d4cd7aa665fc3f9fb71961daa2fded3bb0c6。

Windows交付包含总结、CSV/JSON、PNG/SVG及原始证据ZIP（合成输入、实际app.jar、测量源码、采样/日志/Executor证据）；异常全集与治理原文件保留在WSL实验目录，未全部复制以减少重复占用。源码ZIP不含.env、.runtime或.git。仅本地提交，不推送。

运行期沿用既有检测任务空闲检查；原生导入也应保持空闲，但未增加生产跨服务互斥或全机独占。本轮主导入表起止摘要相同。两次重复、桌面噪声、离散采样、单机共享存储和输入限额均是结论边界。用户不需要手工修复或认证。

## 本阶段结束时的服务

性能测量结束且21个自有试验容器全部停止后，恢复前阶段独立集群演示，地址 http://127.0.0.1:44623/ ，有限会话至2026-10-20 16:34:32（北京时间），到期保留卷。恢复后健康检查及原治理任务读取成功。原主环境仍 http://localhost:15173/ 、local模式，本轮未替换镜像或改写业务数据。演示环境与性能试验相互独立。

最终提交格式检查发现两个新脚本EOF多余空行，已在测量结束后仅清理EOF空白。原测量源码完整保存在原始证据ZIP的measurement-source目录，其摘要与environment.json一致；已核验与最终测量工具除EOF空白外相同。没有重写测量数据或改变已运行逻辑。
