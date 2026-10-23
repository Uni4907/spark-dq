# 主环境待批准发布清单

主环境尚未切换。目标源码提交见SparkDQ-T11-manifest.json的commit字段（发布后二进制受源指纹约束）。

- 后端镜像 `sha256:5582efaa92c4e7809f054bc9a25e0fcf44093668e8c3b44d46f0a6d54a29f91d`；backend.jar `860558dc9a3edfe57f387cb665e31d33ed720432445c2b43d8e2181019b2b707`。
- 前端 `sha256:a718f4705a7d5c48a8db38e0ce0ed2bb41e53f9e131a0adaa3e6988e1651cf07`；Engine `f4006c52ef1689db7fde83deb9a9f3f673434b95b5803357f7ef636ad8819633`。
- 实际旧主库V1—V5→V6，增加业务快照来源字段且放宽两列为空；未删除旧数据。实际主库副本225数据集完整下载指纹及所有业务表行数已核对。
- 实际主环境冷备 `/home/sparkdq/projects/spark-dq/.runtime/t10-main-cold-20261021-223614`，receipt SHA `daa591cbf8ab1fd695be7bf3ed44656a3e061c03c5c94a4f8473bda5f424b10f`；两个tar的SHA/卷内容树记录在该receipt，原主卷前后指纹相同。候选最终恢复点 `87aa5edbbbf4e4f78409516a408e881cf6b18adfe99944720e38f7e47e3d5620`，位于 `/home/sparkdq/projects/spark-dq/.runtime/demo-release-20261021-230214/final-recovery/consistent-backup/backup`。
- 旧镜像+完整切换前冷备已在新卷恢复启动；新版本迁移/候选无源恢复及完整复检通过。正式切换前再次核对主卷是否变化，如变化重建恢复点。
- 回滚在新卷还原元数据库和文件、恢复旧镜像；保留原卷，不只替换旧镜像操作新来源数据。切换期间暂不可用；当前主环境已停止。切换后新增记录如需回滚，须先单独保全，否则可能丢失新增数据。
- 本轮只留下独立演示15174，不改主15173/18080，不删除卷、不推送、不开放公网。

任务书T10明确写明“取得用户明确批准后才能执行主环境切换”。上述清单供审批，不是切换授权；授权前继续使用独立演示。
