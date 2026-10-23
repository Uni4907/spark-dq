# 单机规模性能基准开发记录

时间：2026-10-19T15:50:54+08:00。起点main/717ddec，初始工作区干净。真实项目 /home/sparkdq/projects/spark-dq。本轮不替换主服务镜像、不写主业务库、不删除用户数据、备份或卷，没有远程推送/公开发布。

## 完成范围及改动文件

- spark-engine/WorkerMetrics.java、Engine.java：受限1..32分区环境配置、保留默认输入分区与shuffle=2、真实阶段耗时、累计CPU及Linux VmHWM；仅新增引擎execution结果，不改已有API任务/审计JSON。
- spark-engine/pom.xml、WorkerMetricsTest.java：复用已有JUnit5.12.2的test依赖，4项针对默认值、边界与RSS解析的测试；没有新增生产运行依赖。
- infrastructure/benchmark.py：固定种子生成器、独立预期、计划随机化/预热排除、只读输入/无网络限资源容器、独立worker截止与全组截止、来源/源码/输入指纹核对、原始CSV/JSON与统计。
- tests/test_benchmark.py：8项确定性、预期值、矩阵限制、统计筛选及只停止本次容器的超时测试。
- infrastructure/plot_benchmark.py、benchmark-plot-requirements.txt：已校验COMPLETE数据的独立PNG/SVG/PDF绘图，开发环境依赖隔离，不加入业务服务。
- README、roadmap、benchmark.md、development.md、本日志及本轮完整交付：实验方法、结果、边界与后续。

## 本轮实际测试

Maven backend/spark-engine package成功：原55项Java+新4项引擎测试，共59项，0失败/错误/跳过。Python原48+新8，共56项通过。

200行先导6条件全部通过。正式矩阵固定local[2]、2CPU配额、2GiB容器/1GiB堆，输入分区1/2/4、shuffle2；1万/5万/20万行，3种工作量，27条件各2次，54次测量，另3次预热全部通过。实际输入：10000行=0.67 MiB、50000行=3.40 MiB、200000行=13.66 MiB。

全部试次核对独立预期的所有指标、异常JSON数量/字段、治理审计和CSV多重集摘要。真实Spark版本统一3.5.8；输出规模和预期一致，治理20万行保留10万行并真实文件复检。每次CPU/RSS和阶段耗时取自实际JVM，未使用估计或填0。

| 工作量（20万行、输入分区1） | 两次中位秒 | 最小～最大秒 | 输入行/秒 | 峰值RSS MiB |
| --- | --- | --- | --- | --- |
| 基础3规则 | 9.030 | 9.023～9.036 | 22149 | 503.1 |
| 扩展7规则 | 11.526 | 11.376～11.677 | 17351 | 713.2 |
| 治理+7规则复检 | 14.160 | 14.158～14.161 | 14125 | 646.3 |

完整27条件和全部57次记录见原始summary.csv/measurements.csv。墙钟包含进程/Spark启动、读取与重分区、计算和异常落盘，治理另含输出CSV与复检；不含上传、排队、MySQL、create或Python校验。CPU/RSS在结果生成时采集、不含关闭阶段。新JVM并非冷宿主页缓存，两次重复只报告中位数和范围，无置信区间或显著性承诺。

候选镜像在主计时全部完成后串行恢复新项目/新卷，运行MVP和21项任务的v4完整验收，通过默认requestedInputPartitions=0、实际输入1、shuffle2。默认配置未改旧口径。受保护重启后策略、审计、版本CSV和报告核对通过；隔离项目已停止。记录 /home/sparkdq/projects/spark-dq/.runtime/benchmark-api-97d31326c22d/api-verified.json。

随后实际1秒worker截止故障探针通过：退出码124，本次自建容器停止，失败试次无measurement/COMPLETE，主数据不变。故障证据 /home/sparkdq/projects/spark-dq/.runtime/benchmark-fault-47e988ef8cf1。没有只用mock声称真实超时已经验证。

## 数据与运行边界

正式前后12张业务表全行摘要与文件清单/内容摘要相同，所有实验输入和3个源码指纹在试次后保持一致。主系统仍为上一阶段717ddec运行镜像，后端容器ID与上一部署回执相同，不把新源码或候选镜像当成已部署。当前仍213数据集、269任务，schema2、活动任务0；新增API测试只在独立副本。

每次容器只挂合成输入与本次输出，网络关闭、只读根、限CPU/内存，独立GNU timeout最迟310秒停止；可处理中断只停止匹配本次label的容器，整组默认30分钟。停止容器与所有证据保留，不清理旧备份。容量初查未知时拒绝，之后实际验证D盘VHDX位置及可用空间后才开跑。

## 遇到的问题

宿主原容量入口未返回known=true，预检正确拒绝；实际核对D:/WSL/SparkDQ/ext4.vhdx所在盘后记录真实余量，未用Linux虚拟余量代替。先导校验起初把重复异常当作只有原字段，实际包含__dq_group_size；严格补充该字段和组大小核对后，重新完整6条件通过。失败日志保留，不计入正式54次。

绘图依赖首次使用受限默认临时目录失败，改用工作区专用临时/缓存及目标目录后安装成功；仅工作区绘图依赖，没有修改系统Python或业务镜像。

## 证据与下一步

完整原始运行目录：/home/sparkdq/projects/spark-dq/.runtime/benchmark-matrix-afc7873b9219，其中environment/plan/datasets/oracle、cases逐次日志/请求/结果、measurements/summary和COMPLETE均保留。导出统计与精选原始证据，不含.env、控制密钥、业务SQL或主文件卷。所有原始异常文件仍保留在WSL该目录，未为了压缩包而删除。

本阶段完成20MiB以内单机基准，未完成更大格式/分区输入或集群。后续应以这份基线验证多文件/Parquet输入与执行开销，再推进共享存储、集群提交及状态协调，增加重复次数和专用环境，最后整理论文实验/答辩材料。

提交前仅清理基准脚本第48/74行末空格；保留实测原样的3份源码，并核对原摘要及Python AST一致，原样源码和格式回执另附证据包。该格式整理发生于计时完成后，不改测量/统计。
