# local / 同机 Standalone 配对基准

## 这项实验回答什么

比较相同 Spark 引擎/JAR、相同合成 CSV、规则、治理策略及输入/shuffle分区下，local[2] 与固定双 Worker Standalone 的结果一致性、一次新作业端到端耗时和采样资源占用。仅运行 worker，不调用业务写接口，不改主库和数据文件。它是部署形态对照，不是集群加速的预设结论。

默认1万/20万行，extended（7规则）和governance（7规则+标准化/映射/填充/去重/输出复检），每条件每模式2次正式测量，共16次；先各跑1次最小规模extended预热，共18次。配对顺序及模式先后用固定种子随机化；每次新Driver JVM，集群Master/Worker守护进程复用，但Executor随应用新建。预热不代表复用SparkSession，不清宿主缓存。CSV生成与完整独立预期沿用benchmark.py。

## 控制项与不可等同项

| 项目 | local | Standalone |
| --- | --- | --- |
| 镜像/JAR | 同一锁定摘要官方Spark3.5.8、同一引擎JAR | 相同 |
| SQL并发 | local[2] | 2 Executor，各1声明核 |
| 作业侧CPU配额 | 2 CPU | Driver 0.5 + Worker各0.75 = 2 CPU |
| 作业侧容器内存上限 | 5GiB | Driver 2GiB + Worker各1.5GiB = 5GiB |
| 请求的Java堆上限配置 | 2560MiB | Driver1024 + Executor各768 = 2560MiB |
| 额外控制面 | 无 | Master 0.5CPU / 512MiB；Worker守护进程位于各Worker预算内 |
| 输入/shuffle分区 | 8 / 2，AQE关闭 | 相同 |
| 本地性等待 | 0s | 相同 |
| 网络 | none，自身hostname显式映射回环 | 仅自有internal网络，无发布端口 |

本轮Runtime.maxMemory观测为local 2560MiB、远程Driver约989.875MiB；Driver配置为1024MiB。配置与运行时观测不同，Executor的768MiB仅为请求值；没有固定GC策略。

配额相同不等于每个计算线程可用CPU、GC/native内存、读文件位置、文件缓存归属或进程数量相同。集群有额外Master，Driver/Worker配额拆分也影响调度。本轮没有扫描最优配额，不能把结果解释为所有部署条件下的上下限。正式测量不含集群守护进程启动时间，包含每个应用的新Driver/Executor初始化；这是已启动集群接作业的场景。

CSV仍采用既有multiLine读取后重分区，不能用“8输入分区”断言每个物理阶段均并行。每次检查两个Worker都有成功Task且失败数为0；仅注册两个Executor不算参与证据。共用[官方Spark配置](https://spark.apache.org/docs/3.5.8/configuration.html#scheduling)中的spark.locality.wait=0s，使本轮不因等待数据本地性而推迟非本地任务；该值是本试验固定条件，不修改生产配置。

## 时间与资源

- containerWallSeconds：从docker start前到docker wait确认退出，包含JVM/Spark启动、实际数据读取/重分区、检测及所有异常写出；治理还含保存CSV及真实文件复检。轮询精度约0.25秒，观察和空闲检查会带来少量额外开销。不含创建容器、数据生成、主库指纹、独立预期计算、结果核对、上传、HTTP排队或MySQL写入。
- driverExecution：引擎原有启动、读取/计数、治理/输出计数、规则/异常写出计时；远程模式的workerCpuMs与peakRssBytes只描述Driver，不能冒充整个集群。
- sampledCpuSecondsLowerBound：cgroup v2 cpu.stat的累计CPU增量之和，local含作业容器，cluster含Driver/两Worker/Master；守护进程在作业开始前取基线。容器退出后cgroup可能消失，使用最后一次有效累计值，因此是采样到的下界，缺失不伪造为0。
- sampledAggregateMemoryPeakBytes：约0.25秒读取memory.current，同一采样时刻各相关容器之和的最大值，不能把各自不同时间的峰值相加。含进程/native及cgroup文件缓存，不是RSS/堆，也不是整个宿主峰值；离散采样可能错过尖峰。两Worker/Master常驻内存计入集群值。

所有原始试次和采样保留。中位数及min/max仅描述本机每组两次观测，不是置信区间，不进行显著性推断。用户桌面后台活动、文件缓存、CPU频率和系统噪声不受完全控制。

## 正确性与完成门槛

每次核对全部7规则指标、异常条数/字段、Spark版本、输入/结果分区、真实Executor任务证据；治理完整审计及输出CSV规范多重集SHA与独立预期相同。每个local/cluster配对的规则指标和全量异常多重集SHA也必须相同。独立预期验证的是指标及治理输出，异常内容的全量校验是跨模式对照，不夸称全部异常值已有独立第二实现。

所有计划试次成功、原输入指纹/源JAR及测量工具源码未变、主业务表/文件快照未变后，才写measurements.json、summary.json/CSV及COMPLETE摘要。不完整/失败试次保留容器与日志，不混入最终组统计。绘图再次核对COMPLETE中的summary和measurements摘要。

## 运行

使用既有构建产生的引擎JAR，无需替换或重建主服务镜像。先确认主服务空闲、停止本聊天自有的演示会话（保留卷），不要停止归属不明的服务。本工具拒绝主环境三个容器之外的任何已运行容器。

```sh
# 从仓库根目录执行；容量JSON须是刚核实的Windows后备盘结果。
timeout --signal=TERM --kill-after=90s 1890s python3 infrastructure/cluster_compare.py \
  --output "$PWD/.runtime/cluster-compare-new" --host-capacity /path/to/verified-host-capacity.json \
  --rows 10000 200000 --repeats 2 --max-seconds 1800
```

输出必须是.runtime下全新目录；rows100..250000、1..3次重复、最多40试次，输入仍受20MiB限制。工具需要具备Docker和cgroup读取/试验文件chown权限的执行用户。Windows容量可用交付的runtime.ps1 capacity入口采集；known=false拒绝。运行期沿用现有benchmark.source_idle监测主检测任务，起止比较完整业务表和数据文件；本机原生导入也应保持空闲，本轮未并发用户导入。它不锁定整个桌面或替代生产调度器。

全局试验锁、固定label和自有容器检查避免误操作；对主环境只读。每次作业受父控制器300秒和全组最大1800秒控制；容器内再有1800秒独立终止上限。父进程可处理信号时停止自有容器，强制SIGKILL/断电无法执行finally。只有本次新建且标签匹配、端点已空的网络才删除；数据、停止的容器与日志保留，不做全局清理。

为绕开Docker默认地址池耗尽，读取当前Docker子网及主机路由，从有限候选RFC1918 /28中选择无重叠网段；无可用网段则拒绝，不改旧网络/路由。实际规划保存在network-plan.json。主任务在检查间隔内新启动时仍可能短暂共享资源，无法提供零干扰保证。

## 导出与绘图

保留原始实验目录；可将COMPLETE、summary.json、measurements.json原样复制到独立绘图目录，再运行：

```sh
python3 infrastructure/plot_cluster_compare.py /path/to/complete-evidence --output /path/to/new-figures
```

绘图使用已有benchmark-plot-requirements.txt中的开发依赖，输出PNG/SVG与来源摘要，不加入Java业务链路。图表只在测量结束后生成，避免绘图占用测量资源。最终实际结果及失败试次见本阶段开发日志。
