# 5万/20万行 HTTP 闭环验收

本工具在全新空隔离候选环境中，串行执行两规模×CSV/Parquet×检测/治理/复检。业务执行仍是 Java/Spark；Python 仅提交 HTTP、生成合成夹具并独立核验结果。主环境不写入，不调整上传限额、任务预算或工作节点配额。

## 运行

先按 [隔离环境指南](cluster-lab.md) 检查 Windows 后备盘和 WSL 空间均至少8GiB、主任务空闲以及内存余量，使用实际候选镜像创建新目录。镜像须已存在，命令不自动拉取：

```sh
timeout 90s python3 infrastructure/cluster_lab.py create --output "$PWD/.runtime/scale-lab-NEW" --host-capacity /mnt/c/path/to/current-capacity.json --backend-image spark-dq-backend:header-api-20261020
python3 infrastructure/cluster_lab.py serve --output "$PWD/.runtime/scale-lab-NEW" --seconds 5400
```

serve 是有限控制进程。在另一非交互进程执行以下命令；ACTUAL_SHA256 必须是候选后端容器实际 Engine JAR 的 SHA256，不能仅凭镜像标签推断：

```sh
timeout --signal=TERM --kill-after=30s 2200s python3 infrastructure/cluster_scale.py --lab "$PWD/.runtime/scale-lab-NEW" --output "$PWD/.runtime/cluster-scale-NEW" --engine-sha256 ACTUAL_SHA256 --sizes 50000 200000 --max-seconds 2100
timeout 90s python3 -m unittest discover -s tests -p 'test_*.py'
```

只接收不重复的50000/200000两种规模；可单独验收一种，整段运行上限600..2400秒。每个任务最多等待350秒发布终态及清理结果；后端准备与检测仍共享原300秒预算，未延长。源文件及合成导入须满足20MiB限制。治理去重保留一半行，治理后复检预期按原始规模推导，不会误将治理输入规模再次减半。

## 通过条件与证据

- 每种规模、每种格式各一份检测、治理和复检，任务身份不可复用；完整矩阵缺失或重复不得算通过。
- 七规则 API/Engine 指标、全量异常行多重集、完整治理文件及操作审计匹配独立预期。治理后仍无效的数值按真实规则保留，不能宣称所有异常清零。
- 每个成功任务准备清单 verified/SHA 绑定，执行回执正常结束、两个 Worker 实际完成 Task，结束清理 CLEAR。
- 初始、每个闭环和最终空闲检查，任务队列/Executor/Engine 子进程归零；六服务身份、重启及OOM检查。逐秒 cgroup 内存含缓存，不作为纯Java堆或RSS。
- 最终重新读取历史任务/报告及原始、治理版本下载指纹；主数据恢复点/文件/镜像与运行前一致。

结果目录保留逐试次完整请求/准备/结果/异常/治理/日志/回执、资源采样、主环境只读摘要和汇总。COMPLETE绑定summary/records/resource-samples三个摘要；不存在或摘要不匹配即不能作为完整验收。中断只取消本工具已记录的任务，保留所有输出；SIGKILL无法执行finally，需核对该隔离项目状态，禁止全局清理。重试用全新目录，不覆盖原实验。

durationMs 来自持久化任务，包含准备、检测及结果登记。300000-durationMs 仅为保守观察指标：后端预算在工作进程处检查，状态发布与清理另有开销，所以负余量必须原样报告，不截断或冒充正常的超时注入。一次串行矩阵没有统计重复，不支持CSV/Parquet速度优劣、生产容量、并发吞吐、跨物理机能力或超时故障恢复结论。实际受控超时注入仍需单独实验。

有限会话到期只停止自有容器，保留数据库和文件卷。每次从status读取当前动态地址；旧地址不能当作仍可访问的保证。源码/证据交付不得包含私有.env、数据库密码或主库原始数据。
