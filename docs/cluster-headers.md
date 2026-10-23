# CSV表头优化复现

运行前准备固定Docker镜像、Java17/Maven构建、主环境健康且没有任务/原生导入；停止自有演示后再测量。不要关闭别人的作业。主库仅作只读起止核对。

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
python3 infrastructure/cluster_headers.py \
  --output /home/sparkdq/projects/spark-dq/.runtime/cluster-headers-NEW \
  --baseline-run /home/sparkdq/projects/spark-dq/.runtime/cluster-formats-20261020-a \
  --baseline-sha256 16a51e1bf1eb11a0623b31bd6ea6bd196941bd6735f21773f3f9075bcae5decd \
  --host-capacity /path/to/current-windows-capacity.json --repeats 2 --max-seconds 1800
```

output必须是.runtime下全新目录；baseline-run必须是有COMPLETE的前阶段目录，保留原app.jar及inputs/200000-csv、200000.csv；候选JAR读取spark-engine/target/spark-engine-0.1.0.jar。Windows容量收据来自已交付SparkDQ-runtime.ps1的capacity动作，要求至少8GiB空闲；同时检查Linux容量。Maven构建用项目锁定maven:3.9.12-eclipse-temurin-17镜像，详见前阶段指南。

图表可用开发依赖Matplotlib运行：`python3 infrastructure/plot_cluster_headers.py --input .runtime/cluster-headers-NEW --output /path/to/new-figures`，工具先验证COMPLETE摘要与成功状态。

默认22次兼容性/拒绝运行+4预热+16正式A/B。每作业有限等待，外层建议有限超时1870秒；各容器独立1800秒保险。异常退出触发finally，仅停止标签匹配的本次容器，保留文件/容器；空自有网络才回收。主任务或原生导入出现时停止本实验，不取消用户任务。超时/失败目录没有COMPLETE，不能作完整实验。

COMPLETE绑定summary、measurements及compatibility SHA；environment绑定两版JAR、源文件及固定环境。每次换JAR前确认本次Driver已退出、Worker无Executor；不同版本只替换新实验目录内的app.jar。完整异常及治理内容匹配独立预期，而非只对照两个实现。每组n=2只作描述统计，不排除慢样本。

标准表头快速路径只识别已校验ASCII列名、完整字段/记录边界。其他输入仍交Spark原解析器；它不是新CSV解析器或跳过完整性检查的开关。主环境/演示镜像均未更新。源码及日志说明见development-2026-10-20-cluster-headers.md；当前完整进度见progress-delivery-cluster-headers-2026-10-20.md。
