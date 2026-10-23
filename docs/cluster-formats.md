# 三种执行输入的同口径对照

## 范围

在同机local[2]与固定双Worker Standalone上，使用相同合成源CSV、7条规则和治理策略，比较单CSV、四分片CSV及四文件全字符串Parquet。默认20万行，每个模式/流程/格式重复2次，共24次正式测量；另有两次单CSV预热和两次独立准备，共28次真实Spark作业运行。输入仍在20MiB边界内，不代表TB级或跨物理机实验。

不是原生Parquet上传实验：CSV先由现有Java PreparedInput生成副本，做完整多重集回读，冻结manifest SHA后进入检测。对照的是现有检测/治理执行输入及其验证成本，不是绕过完整性检查的裸文件扫描。

## 固定条件

沿用cluster-compare.md：同一锁定Spark3.5.8镜像与同一引擎JAR；local作业2CPU/5GiB、请求堆2560MiB；cluster Driver0.5CPU/2GiB、请求堆1GiB，两Worker各0.75CPU/1.5GiB、Executor请求堆各768MiB，额外Master0.5CPU/512MiB。聚合配额相同不等于GC、文件缓存、进程数量或逐线程预算相同；Runtime.maxMemory与请求-Xmx不混同。

检测/治理输入分区统一请求8，shuffle2，AQE关闭，locality.wait=0s。每个新Driver/Executor独立启动，守护进程复用；格式顺序在每个模式/流程/重复组内固定种子随机化，组顺序也随机。预热仅为最小输入的单CSV，不清除OS缓存；转换过程会影响文件缓存。每组n=2，只报告描述统计，不作显著性推断。

## 读取分区观察

引擎worker结果execution增加sourcePartitions（应用可选repartition之前的RDD分区数）和repartitionApplied，原有requestedInputPartitions/inputPartitions/resultPartitions保留。没有额外count来测量此字段；不记录原始值或完整执行计划。不新增数据库版本或用户可调Spark参数。

sourcePartitions只说明读取计划暴露给RDD的分区数，不等于同时运行的Task数。四个文件也可能被Spark合并为少于四个扫描分区；缓存/重分区/治理后分区还会改变。每次仍检查最终输入8分区，以及两Worker都有实际成功Task且失败为0；Executor注册数量不能替代参与证据。

CSV分片在进入正式读取前逐文件验证表头；Parquet需要schema和保留空值标记验证；各分片的文件SHA/行数等验证也计入正式路径。不能用这套端到端时间推断裸CSV/Parquet扫描带宽，也不能直接定位唯一瓶颈。

## 转换与复用模型

每个行数分别准备CSV/PARQUET一次，独立local[2]、2CPU/5GiB、请求堆2560MiB。转换墙钟包含新JVM、写出和全部数据回读验证，不含复制已验证文件到试验输入目录。分片数据字节数不含清单、CRC及_SUCCESS等旁文件；原始CSV及准备目录保留。

实际测量为：准备一次、每条件两个新作业。另计算成本模型：

- C：该格式准备墙钟；B：同模式/流程的单CSV中位作业墙钟；P：该格式的中位作业墙钟。
- 单次总成本：C+P；N次复用模型：C+N×P；单CSV对照N×B。
- 仅P<B时存在严格摊平次数，最小正整数N=floor(C/(B-P))+1；相等不算胜出。P≥B则模型没有有限交点。

这是基于两次作业与一次转换观测的模型，未独立执行N次验证，转换也没有重复估计方差。现有业务没有新增跨任务共享执行副本缓存，因此不能宣称模型收益已自动落到主系统。重用需要沿用被冻结、完整性验证过的同一输入；不是修改或覆盖原文件。

## 时间、资源及正确性

复用前阶段的容器墙钟、约0.25秒cgroup CPU累计下界/同时刻容器memory.current之和峰值、Driver阶段计时；集群资源采样包括Master/两Worker，CPU下界与内存采样都不是精确总CPU/RSS。格式转换单独计时，绘图在全部测量结束后执行。

每次与独立Python预期比较全部规则指标、治理审计及完整治理CSV多重集；所有模式/格式/重复的异常全集SHA也须一致。准备工具已有完整原CSV与副本exceptAll双向核对。输入/准备文件全指纹及测量源码/JAR冻结；全部试次正确且主数据/文件恢复点一致后才生成COMPLETE。失败不进入统计，不重写旧试验目录。

## 执行

先按主README的固定Maven镜像执行package，确保最新引擎JAR生成，再运行本工具。不修改主服务镜像。主服务保持可读且空闲；先用cluster_lab.py stop停止本聊天拥有的演示租期，保留卷。不要停止归属不明的容器。

```sh
timeout --signal=TERM --kill-after=90s 2490s python3 infrastructure/cluster_formats.py \
 --output "$PWD/.runtime/cluster-formats-new" --host-capacity /path/to/fresh-host-capacity.json \
 --rows 200000 --repeats 2 --max-seconds 2400
```

Windows后备盘容量必须已核实，known=true且至少8GiB，Linux也保留至少8GiB。主环境三个容器之外有其他已运行容器即拒绝。共享基准锁，使用固定标签和不与Docker子网/主机路由重叠的内部/28，无发布端口；只停止自有容器并回收自建且已空的网络，保留卷、文件与退出容器。

每次作业父控制器300秒，准备310秒；测量预算最高2400秒，外层命令另有限时。继承的容器独立超时1800秒，Master/Worker也受此上限；慢机器、大计划可能先触发节点期限并失败，不能保证所有合法计划在预算内完成。使用全新目录重跑，不合并失败试次或省略正确性门槛。

本阶段覆盖主检测与原生导入空闲检查：开始、试次边界及每约40次采样检查；有活动用户工作时停止实验，不取消用户任务。没有全机独占或生产调度互斥，检查间隔内仍可能短暂共享资源，最终业务/文件对账也会拒绝有变化的试验。

## 产物与绘图

plan/environment/preparations、每个request/result/measurement/资源采样/Driver日志/Executor证据、summary.csv/JSON、measurements.json、COMPLETE均保留。Java元数据变化运行全量Maven回归，Python新工具运行已有全部运维测试，实际结果以本阶段交付为准。

```sh
python3 infrastructure/plot_cluster_formats.py /path/to/complete-evidence --output /path/to/new-figures
```

绘图只接受摘要校验通过的COMPLETE数据；使用已有开发绘图依赖，输出PNG/SVG和来源SHA。左图为作业中位数/min-max，右图为准备一次加作业中位数，无转换方差推断。源码、原始证据ZIP及完整进度另行交付，Python仍只承担运维/实验，不改变Java/Spark业务架构。
