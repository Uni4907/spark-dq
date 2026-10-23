# 受控连续运行验收

使用已交付cluster_lab.py创建全新空候选环境，保持主环境健康且无活动工作，容量检查至少8GiB；不要复用含用户数据的演示库。镜像选择和有限会话见cluster-lab.md。启动有限会话后，在另一个非交互进程运行：

```sh
timeout --signal=TERM --kill-after=30s 1870s python3 infrastructure/cluster_stability.py --lab "$PWD/.runtime/soak-lab-NEW" --output "$PWD/.runtime/cluster-stability-NEW" --engine-sha256 ACTUAL_SHA256 --rounds 3 --rows 10000 --min-seconds 900 --max-seconds 1800
```

要求.output在.runtime下全新目录；1..4轮、偶数1000..20000行，60≤最短窗口<最大窗口≤2400秒。默认3轮/1万行/至少900秒。任务等待、外层进程和环境会话均有限时；出错只取消本轮记录的活动任务，保留工件。没有COMPLETE不能作为完整通过结果。失败后重试使用新空实验，不删除旧数据。

三轮两格式各检测/治理/复检=18成功，另外阻塞任务/取消后重放=2成功；排队与运行取消各1。采样六个持续运行的容器cgroup，服务PID/重启、CPU重置、OOM增长为失败信号；内存增长本身不判作泄漏。11个空闲检查确认队列/资源门禁/进程，保存下载SHA与历史任务和报告。结果目录包含全量异常/治理数据和逐秒资源，不只是预览。

```sh
python3 -m unittest discover -s tests -p 'test_*.py'
python3 infrastructure/plot_cluster_stability.py --input .runtime/cluster-stability-NEW --output /path/to/new-figures
```

图表需开发依赖Matplotlib，先验证COMPLETE。纵线为空闲检查，内存含文件缓存，CPU为六服务采样累计；单次短时结果不能证明多日稳定、泄漏不存在或容量上限。原始采样、结束摘要、源码和镜像ID一起留存。临时网络、所有卷和记录保留；没有主部署切换/远程发布/全局清理入口。
