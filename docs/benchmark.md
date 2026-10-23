# 可复现单机规模基准

本工具运行真实Spark引擎，使用独立限资源容器和新生成的合成数据，不通过主系统入库。主数据库、数据集、任务与文件不改写。每次试验独立JVM，资源和结果原始证据保留；仅验证正确的试次才进入统计。

## 实验设计

默认固定种子20261019，CSV为10000/50000/200000行，8个字段，原始字节不超过本系统20MiB上传边界。每两个记录共用业务键；固定周期注入NULL、空字符串、普通空格、非法整数、越界数值和状态格式问题，备注使用有种子的随机字节。记录CSV字节数与SHA-256，生成器和独立Python预期均在infrastructure/benchmark.py。

| 控制因素 | 配置 |
| --- | --- |
| 执行器 | 独立Spark3.5.8 JVM，local[2]，每次新进程 |
| 资源预算 | Docker 2 CPU配额、2GiB内存/无额外交换、1GiB Java堆、512进程/线程限制 |
| 输入分区 | 1、2、4，记录实际缓存输入分区；shuffle固定2、AQE保留原默认并记录 |
| 规则/治理 | basic：2空值+1重复，3规则；extended：再加INTEGER、RANGE、REGEX、DATE，共7规则；governance：7规则+标准化/映射/填充/移除/按数值及日期去重→文件复检 |
| 重复与顺序 | 27条件各2次，共54次测量；每输入分区1次最小规模extended预热，共3次；之后固定种子随机排列 |

预热仍是新JVM，不代表复用Spark会话；它只能影响操作系统/文件/镜像缓存。本轮不清除宿主缓存、不锁CPU频率，不控制用户桌面的全部后台活动。主系统活动任务在试次前后检查，若有用户工作则停止实验，不取消用户任务。

CSV读取仍是原multiLine方式。1分区可保留原读取计划，2/4分区明确增加repartition并计入耗时；不要把分区增加和无成本并行等同，也不要把输入分区数当成所有SQL阶段的物理分区数。治理保存仍coalesce为单CSV，复检文件分区另行记录。

## 计时与资源口径

- containerWallSeconds：docker start到容器退出，包括JVM/Spark初始化、读取/重分区、规则计算、全部异常JSON落盘；治理还包括新CSV保存及读取实际文件复检。不含容器create、上传、任务队列、MySQL持久化、Python预期核对。
- workerElapsedMs：Java main入口到结果生成前，另有Spark启动、输入读取/计数、治理与输出计数、规则/异常写入分阶段耗时。
- workerCpuMs：JVM进程累计CPU时间；多线程CPU时间可高于墙钟时间，不是“CPU利用率”。
- peakRssBytes：Linux /proc/self/status 的VmHWM，到生成结果时的进程历史峰值，含native，不等于堆或整个容器内存。CPU/RSS采样不包含随后关闭阶段；缺失时不编造0。

工具产出原始measurements.csv、每条件中位数/最小/最大值的summary.csv和JSON，保留全部试次，包括预热。图中误差线是两次测量的最小～最大范围，不是置信区间；不进行显著性推断。

## 正确性门槛

每次核对独立预期的全部规则指标、每规则异常文件条数和字段（重复异常另含组大小）、治理审计及输出CSV的排序规范多重集SHA-256。预期按明确生成规律独立计算，不从Spark结果反推。治理每组优先保留score=10的后一条，输出约为输入一半；指标是保存后的真实CSV复检值。

输入只读挂载，试次后校验输入指纹；主库12张业务表和文件在整组前后对账相同。采集源码摘要、实际引擎JAR摘要、镜像不可变ID、Docker/内核/Python环境、宿主容量和精确运行计划，结束时检查实验源码未中途修改。只有完整成功后才写COMPLETE及summary摘要，不将失败或被中断的试次凑成结论。

## 有界执行与数据保护

新输出目录只允许位于项目.runtime之下，拒绝覆盖已有目录；最多100个试次、每次最多300秒（容器内独立timeout再加10秒强制终止），整组默认1800秒，最多3600秒。进程可处理中断会只停止本次label验证过的容器；即使父工具强制终止，容器内截止机制仍有效。只读根文件系统、无网络、降权能力、不挂主文件卷或数据库。

停止的实验容器和证据保留，不自动删除文件或卷；失败时无COMPLETE。磁盘需要预留空间：Linux虚拟空闲量不等于Windows实际余量，WSL建议传入已核实的宿主容量JSON，known=false的输入会拒绝。容量记录须包含known=true和freeBytes，可附盘符、VHDX位置与核实时间；本轮已单独核对真实VHDX所在盘。

## 运行与重现

在项目WSL根目录，以具备Docker访问权限的当前运行用户执行：

~~~bash
# 先按已有构建入口编译，构建单独的实验镜像；不覆盖主服务镜像标签
docker run --rm -v /home/sparkdq/projects/spark-dq:/src -v spark-dq-maven-cache:/root/.m2 -w /src maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d mvn -B package
docker build -f infrastructure/backend.Dockerfile -t spark-dq-backend:benchmark .
timeout 1900 python3 infrastructure/benchmark.py run --output .runtime/benchmark-reproduce-01 --host-capacity /path/to/verified-host-capacity.json
~~~

可改--rows、--partitions、--repeats、--warmups、--seed、--profiles、--max-seconds；所有配置写入计划。重新运行必须使用新目录，不修改旧试次。不要在同一台机器并发另一套性能负载。

新引擎可选SPARK_INPUT_PARTITIONS=1..32、SPARK_SHUFFLE_PARTITIONS=1..32；不设置输入项保持原读取分区，不设置shuffle保持原2。HTTP接口没有开放任意Spark参数，新execution元数据保留于引擎result.json，不改变历史API任务/审计形状。正式主服务是否切换到新镜像，必须依据本阶段交付记录，不因为源码存在就假设部署完成。

绘图用独立环境安装infrastructure/benchmark-plot-requirements.txt，再执行：

~~~bash
python3 infrastructure/plot_benchmark.py .runtime/benchmark-reproduce-01 --output .runtime/benchmark-figures-01
~~~

输出延迟/RSS的PNG、SVG、PDF，可用于后续论文整理。绘图只读取已核对COMPLETE的数据，不影响测量。

本阶段是本机、20MiB以内样本的首轮受控基准，不能宣称分布式/TB级性能。后续应增加重复次数、专用环境、更多规模和分区、Parquet等格式与真实集群；优化须基于本轮证据，并继续保留正确性门槛。
