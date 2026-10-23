# 多文件 CSV / Parquet 引擎输入基础

本说明记录3c2a4eb离线基础阶段；后续已完成的任务接入见 [执行副本](execution-inputs.md)。该基础阶段范围是离线准备与独立 Spark worker。网页上传、数据库 datasets.file_path、任务快照和治理输出仍是原来的单文件 CSV；没有开放 HTTP 文件系统路径或 Parquet 上传，也没有替换主服务镜像。

## 输入契约

历史 request.input + csv-v1/csv-v2 保持兼容。新请求必须是 csv-v2，使用 inputManifest 和 inputManifestSha256，且不能同时出现 input。清单仅 v1，format 为 CSV 或 PARQUET；包含 sourceSha256、唯一有序 columns、rowCount 及逐文件 path/size/sha256。

清单文件最多1 MiB，1..100个合法字符串字段、1..256个文件，总文件大小最多256 MiB。相对路径限 data/part-* 加匹配扩展名；不接受目录扫描、通配符、重复路径、穿越或符号链接。worker先验证清单指纹和每个文件的字节指纹；只读取列出的文件，未列出文件不参与计算。读取后验证实际行数。CSV先逐文件独立读取表头并与清单严格对照，再用显式字符串字段、RFC4180、enforceSchema=false、FAILFAST读取。真实故障验收发现仅依赖Spark合并多路径读取并不足以检查每个表头，已补充该逐文件检查；Parquet逐文件校验字段名称/顺序和全字符串类型，避免 mergeSchema=false 隐藏缺字段。NULL、空字符串、空格沿用 csv-v2；非空的精确保留标记 \N 在 Parquet 输入中拒绝，避免后续 CSV 治理输出丢失语义。

请求与清单应来自可信的离线控制过程。指纹用于发现变更，不能证明提供者身份；校验后到读取期间依赖文件不可变/容器只读挂载，不宣称对有权限的并发文件替换者具备事务隔离。

## 准备与可复现验收

Java入口 cn.sparkdq.engine.PreparedInput 接受一个JSON请求文件：

~~~json
{"input":"/benchmark/input/source.csv","output":"/benchmark/output/prepared","format":"PARQUET","partitions":4}
~~~

支持 CSV/PARQUET，分片请求1..16；输入是最多20 MiB本地单文件 csv-v2，输出必须是新目录。读取并缓存源数据，分片保存后双向 exceptAll 核对完整行多重集，再检查源SHA未变；成功后原子发布 manifest.json。preparation.json 记录实际分片数量、SHA与准备耗时，不能假定小数据一定产生指定文件数。失败保留局部目录，不能继续当作成功清单使用，不覆盖原文件/旧目录。

开发构建：

~~~bash
cd /home/sparkdq/projects/spark-dq
timeout 600s docker run --rm -v /home/sparkdq/projects/spark-dq:/src -v spark-dq-maven-cache:/root/.m2 -w /src maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d mvn -B package
timeout 600s docker build -f infrastructure/backend.Dockerfile -t spark-dq-backend:prepared .
~~~

复现实验（需要Docker权限；host-capacity.json来自实际 Windows VHDX 所在盘，包含 known=true/freeBytes/checkedAt，不能拿WSL虚拟空闲容量代替）：

~~~bash
timeout 1860s python3 infrastructure/prepared_inputs.py \
  --output /home/sparkdq/projects/spark-dq/.runtime/prepared-NEW-ID \
  --image spark-dq-backend:prepared --rows 10000 200000 --repeats 2 \
  --host-capacity /absolute/path/host-capacity.json --max-seconds 1800
~~~

新建隔离目录；沿用 benchmark.py 固定种子与独立指标/治理预期。每个新容器 local[2]、2 CPU、2 GiB内存、1 GiB堆、shuffle=2；不强制重分区，记录实际输入分区。禁网络、只读根目录/输入、cap-drop ALL、有时限，只停止本次标签匹配的容器。Snappy需要映射本地库，因此仅容器临时tmpfs显式exec；没有给主服务改挂载或主机设置。Docker stdout/stderr完整合并保存 engine.log。主业务行/文件前后核对，COMPLETE仅全部通过后生成，失败目录保留。

每条件两次新JVM，随机顺序，无独立预热，不控制主机文件缓存。总耗时包括本次指纹检查/读取、Spark启动、规则和异常落盘、治理保存与CSV复检，不含容器create、Python核对、上传/队列/MySQL。转换准备耗时（包括双向保真核对）单列并给出“准备+一次运行”，避免仅报告后续读取更快。读取阶段包含清单/文件指纹、CSV逐文件表头、Parquet逐文件schema以及保留标记校验。异常行完整多重集在三格式间核对；不把跨格式一致替代独立指标预期。

特殊样本覆盖NULL/空/纯空格、中文/emoji、引号/逗号、LF/CRLF、前后空格和非精确保留标记文本。真实失败探针包括清单指纹、文件损坏、重算清单指纹的假行数、穿越路径、重算清单指纹的错表头、准备目录已存在；退出必须为1、非OOM/超时、命中特定错误且没有成功结果。

## 后续接入边界

下一步可将准备工件纳入受控数据集注册：数据库迁移、存储清单备份/对账、快照冻结 manifest SHA、排队前校验、取消/恢复与版本引用、HTTP格式选择及页面提示都需一起验收。当前没有绕开这些生命周期约束将离线工件伪装成已注册数据集。没有分布式/TB级验证，上传上限仍20 MiB，任务仍串行、本地Spark。

空版本回归：在没有正在进行的基准实验时执行 timeout 1500s python3 tests/prepared_empty.py --image spark-dq-backend:prepared；该脚本新建独立目录并检查三格式×检测/治理的0行指标、异常、审计和真实CSV输出，不把空版本试次计入规模性能统计。

开发机绘图依赖沿用 infrastructure/benchmark-plot-requirements.txt，与业务镜像隔离。完成实验后执行 python3 infrastructure/plot_prepared.py /absolute/experiment /absolute/new-figures，可校验COMPLETE并导出PNG/SVG/PDF；目标目录必须全新，图中同时显示worker耗时与准备加一次运行。
