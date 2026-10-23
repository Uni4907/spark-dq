# 候选服务验收复现

主环境健康且无活动任务；至少8GiB后备盘及Linux容量；使用现有锁定Maven构建流程。先构建新的本地后端标签，保留原标签：

```sh
timeout 660s docker build -f infrastructure/backend.Dockerfile -t spark-dq-backend:header-api-NEW .
python3 infrastructure/cluster_lab.py create --output "$PWD/.runtime/header-api-lab-NEW" --host-capacity /path/to/current-capacity.json --backend-image spark-dq-backend:header-api-NEW
python3 infrastructure/cluster_lab.py serve --output "$PWD/.runtime/header-api-lab-NEW" --seconds 7200
```

serve是有限前台控制进程；另一个非交互会话运行验收（不要把输出目录改为已有目录）：

```sh
timeout 1850s python3 infrastructure/header_service_acceptance.py --lab "$PWD/.runtime/header-api-lab-NEW" --output "$PWD/.runtime/header-api-acceptance-NEW" --engine-sha256 ACTUAL_ENGINE_SHA256
```

Engine SHA从已打包spark-engine/target/spark-engine-0.1.0.jar计算，验收确认容器内实际文件相同。验收要求新空数据库，建立合成数据后将整套自有服务停启，并在Engine STARTED后SIGKILL本次后端，观察恢复及复检；不用于主环境或已有用户数据实验。等待、子进程、Java任务和会话都有期限；失败保留目录，没有COMPLETE即不算完整通过。首次协议可能产生部分合成任务，重跑使用新空项目，不能删除数据以伪装新项目。

读取当前地址请用cluster_lab.py status；动态端口可在任何stop/start后变化。OBSERVED表示观察到当前端口映射，不等于额外的健康保证；UNAVAILABLE时不返回旧URL。启动日志在私有startup-*.log，不应未经审查复制到公开位置。工具不拉镜像、不删网络或卷、不更新旧项目控制文件。网络若在create和serve之间变化导致冲突，保留失败目录并新建实验，不清理别人的网络。

成功证据COMPLETE绑定summary和fixture；另有每个成功任务的Worker结果/Executor证据、准备清单、中断前后任务JSON、整套停启恢复点。运维测试：python3 -m unittest discover -s tests -p 'test_*.py'。本轮真实结果见开发日志/完整进度；本工具不做性能加速或跨物理机/HA验收。
