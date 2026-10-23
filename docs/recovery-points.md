# 备份巡检与业务恢复点

本阶段修复V5的14表备份被旧巡检错误拒绝的问题，并为新备份增加可复核的业务数据库指纹。不修改旧备份、不回填不存在的历史证据，也不启动定时任务。

## 接口和结果

```bash
# 只读验证备份；不会连接当前数据库、启动或停止服务
timeout 180s python3 infrastructure/backup_catalog.py --root .runtime/指定备份 --verify-latest 1 --output .runtime/catalog-new.json
# 验证后，对照当前本机数据库的一致只读快照
timeout 240s python3 infrastructure/backup_catalog.py --root .runtime/指定备份 --verify-latest 1 --compare-current --output .runtime/coverage-new.json
# 隔离恢复项目可显式指定自己的配置和环境文件；不会把凭证写入报告
timeout 240s python3 infrastructure/backup_catalog.py --root .runtime/指定备份 --verify-latest 1 --compare-current --config /绝对路径/compose.yaml --env /绝对路径/.env --output .runtime/coverage-clone-new.json
```

输出路径必须不存在，拒绝覆盖。--compare-current要求本次完整校验至少一份备份；不以仅元数据检查宣称已验证覆盖。退出码0表示本次请求的检查满足条件；2表示扫描/空间/时效或覆盖条件未满足；1表示操作错误。比较当前数据库失败不会自动启动服务或读取其他项目。

businessCoverage.status的含义：

| 值 | 含义 |
| --- | --- |
| matches | 本次完整校验过的某份备份，其已记录业务指纹等于当前一致快照 |
| changed | 已有可比较指纹，但当前记录或表结构变化；列出变化表及前后数量，数量相同也会报告 |
| unknown | 旧备份没有恢复点，或本次没有可比较的已验证恢复点；不推测已覆盖 |
| unavailable | 当前数据库无法读取；不误报为已覆盖 |

freshVerifiedBackup仍只表达备份的时间/工件完整性。较新并不等于覆盖当前业务，业务覆盖结果单独显示。默认不传--compare-current时完全不访问当前数据库。

## 指纹如何生成

新完整备份在原有维护/停写窗口中取得业务指纹，并将recoveryPoint作为sparkdq-backup-v1清单的可选扩展，受原有manifest/COMPLETE绑定。旧备份没有该字段，仍可校验和隔离恢复，但其业务覆盖状态为unknown。

一次MySQL REPEATABLE READ + WITH CONSISTENT SNAPSHOT + READ ONLY事务中读取全部受支持业务表；会话时区固定UTC。每条业务记录使用MySQL JSON_ARRAY按真实字段序列编码并由MySQL SHA-256，仅输出每行摘要，再按摘要排序并保留重复数量；每表绑定字段类型/可空性/排序规则和记录摘要，最后绑定成总指纹。明文业务值不通过此查询输出到Python或报告。Flyway历史不进入业务指纹，但仍包含在原有完整SQL备份中。

只接受已知12表业务布局及V5新增dataset_imports；备份布局可为旧12表、含Flyway的13表或V5的14表。未知表、异常字段名、坏摘要或不完整响应均拒绝，不生成部分成功结果。最多100000条业务表记录（不是CSV中的数据行）、每表最多100字段、元数据最多2048字段；每个摘要SELECT设置5秒会话执行上限，现有数据库客户端调用也有限时。超过预算会明确失败，后续可按测量扩展，不能静默截断。

新卷恢复仍先验证SQL/文件/实际应用工件，再导入全新的专用卷并对账；若备份含恢复点，应用启动前重新计算数据库指纹并要求完全一致。数量相等但内容不同会拒绝启动，保留新卷、日志及失败回执，不覆盖用户现有卷。

## 范围和限制

业务指纹对账只证明某一时刻的数据库记录一致，不能替代当前文件逐字节审计、实际应用版本/权限/凭证检查，也不表示当前运行中的每条新记录都已经备份。在线比对结束后仍可能有新任务写入，需重新核对。

清单指纹用于发现意外变化，不是数字签名。目录完整性校验不执行备份SQL；SQL内容与业务指纹的实际一致性由隔离恢复检查验证。因此保持完整备份和实际恢复演练，不把“工件校验通过”写成“已演练恢复”。

本阶段仍不删除或自动清理备份、不自动覆盖恢复、不创建开机启动/定时备份，不发布外部服务。Java/Spring Boot与Java/Spark业务架构未改动，Python仅承担已有本地运维与验收。

## 隔离验收入口

`tests/recovery_points_acceptance.py`接受--backup、--output（全新目录）和--fixture（既有native_imports_e2e生成的夹具JSON），可选--watch-primary只读对照主环境。它只在新恢复卷创建合成CSV和修改该合成记录，验证新增/同数量改值、CLI非零结果、新备份新卷恢复和错误恢复点拒绝。所有卷/工件保留，成功结束停止隔离容器；不在主库注入故障。

--resume仅用于复用已验证的首轮恢复与第一份备份，并校验源清单绑定。后续阶段已有证据时会拒绝复用，避免重复创建合成记录或覆盖回执，应使用新输出目录。隔离服务使用随机端口，备份重启后重新获取地址。
