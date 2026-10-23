# 临时交付：稳定性阶段已按用户要求暂停（2026-10-20）

已停止自动测试，并在活动任务清理完成后停止本轮独立实验的全部6个服务，所有卷、合成数据、异常/治理文件和资源采样保留。主环境及此前候选环境未替换；各自原有限时会话不受本次暂停指令改变。没有远程推送或外部发布。

## 已完成并可查证

- 191项运维回归通过，新增10项；Java/前端未改，本轮未重跑Java/Node。
- 三轮CSV/Parquet的检测→治理→复检共18个任务全部通过；另1个用于排队验证的阻塞任务成功，共19份成功任务完整记录。
- 每任务七规则指标、全量异常与治理完整行多重集匹配独立预期，成功任务两个Worker均有实际Task；1万行治理为5000行。
- 已完成排队取消检查；运行取消已发起，暂停收尾确认无活动任务、无待清理门禁。最终状态计数：{"SUCCEEDED": 19, "FAILED": 0, "CANCELLED": 3, "RUNNING": 0, "QUEUED": 0, "CANCELLING": 0}。
- 已保存551个资源样本，跨度约9.18分钟。主数据/文件/镜像与业务恢复点1c2caca9b8dff53db7627c96c051d4cd7aa665fc3f9fb71961daa2fded3bb0c6保持不变。

## 尚未完成，不能算整阶段通过

用户要求暂停时，完整稳定性工具尚未写入COMPLETE。取消后复检重放、最终全量复读、完整观察窗口和最终资源汇总/图表交付尚未全部完成；本轮只有PAUSED.json，不能以部分成功代替完整验收。中断日志中的InterruptedError来自用户主动暂停，不是业务故障。不要运行依赖COMPLETE的导出脚本并宣称通过。

## 保存位置与续接

真实项目：/home/sparkdq/projects/spark-dq，WSL发行版SparkDQ-Ubuntu。

本轮源码：infrastructure/cluster_stability.py、infrastructure/plot_cluster_stability.py、tests/test_cluster_stability.py；Java业务镜像沿用spark-dq-backend:header-api-20261020，Engine SHA fdb678527e494f54411fcaf9809d6e195f4485b134ffbe5265d7fd3f0c720493。

部分证据：.runtime/cluster-stability-20261020-a；停止的独立环境：.runtime/soak-lab-20261020-a（项目spark-dq-lab-02febcc00854）。不要清空这些目录或卷。上一完整阶段提交52a0743，当前临时提交见交付收据。

继续时先只读确认git/运行状态、主环境健康及容量；若有限会话已到期，正常有限续启。建议使用新空环境和新输出目录，例如soak-lab-20261020-b、cluster-stability-20261020-b，从头运行3轮/1万行/至少900秒观察，确保连续窗口没有暂停间隙；旧记录仅作部分证据保留。命令见下方。不要覆盖旧目录或删除数据来制造空库。

```sh
python3 infrastructure/cluster_lab.py create --output "$PWD/.runtime/soak-lab-20261020-b" --host-capacity /path/to/fresh-capacity.json --backend-image spark-dq-backend:header-api-20261020
python3 infrastructure/cluster_lab.py serve --output "$PWD/.runtime/soak-lab-20261020-b" --seconds 5400
# 另一个有限非交互会话：
timeout --signal=TERM --kill-after=30s 1870s python3 infrastructure/cluster_stability.py --lab "$PWD/.runtime/soak-lab-20261020-b" --output "$PWD/.runtime/cluster-stability-20261020-b" --engine-sha256 fdb678527e494f54411fcaf9809d6e195f4485b134ffbe5265d7fd3f0c720493 --rounds 3 --rows 10000 --min-seconds 900 --max-seconds 1800
```

工具会主动取消本次创建的活动任务以收尾；不会取消主环境用户任务。新结果完整后再生成资源图、更新完整进度/开发日志和路线图。Windows工作目录work/soak_export_docs.py、work/soak_export_docs.py等只是尚未执行的导出草稿，当前写死旧实验路径，继续时必须改为新实验并核对实际结果；不要直接重跑。

当前没有必须由用户操作的步骤；等用户明确继续后再开始自动开发。
