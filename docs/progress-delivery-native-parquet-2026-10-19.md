# SparkDQ 当前完整进度交付：原生 Parquet 导入基础

截至 2026-10-19T21:59:23+08:00，本阶段完成原生有类型Parquet的有界离线转换、明确类型/空值契约、全多重集回读，以及转换结果接入真实检测/治理/复检的隔离验证。网页直接上传Parquet与有状态导入仍待下一阶段接入；主环境继续运行上一阶段V4功能。

## 当前完整进度

| 范围 | 已具备能力 |
| --- | --- |
| 网页业务闭环 | 单CSV/2..16份同结构CSV注册→规则/版本化模板→Java/Spark检测→显式治理→独立版本/快照→实际文件复检→前后对比/JSON报告 |
| 检测 | 空值/空字符串/纯空格、业务键重复、整数/精确数值/日期、范围、正则、完整异常和预览 |
| 治理 | 标准化、精确非连锁映射、填充、缺失移除、数值/日期/文本多级去重、逐操作审计 |
| 执行输入 | 原CSV与服务端分片CSV/全字符串Parquet副本，规则/源指纹/执行计划冻结、清单指纹一次性绑定、治理复检继承 |
| 可靠性与运维 | 失败/超时/取消持久化、子进程清理/重启恢复、分页、空闲维护、一致SQL+数据+实际应用备份/校验/新卷恢复、有期限运行会话 |
| 多CSV来源 | 保留原分片/顺序/行数/大小/SHA，校验下载，来源完整性纳入V4备份巡检 |
| 本阶段原生格式 | 原生Parquet精确标量转换为规范CSV、固定源SHA、限额与容器隔离、独立回读/COMPLETE验证、可复现样本和真实业务流程验证 |
| 实验材料 | 历史单机1万/5万/20万行基准及格式对照保留；本阶段是正确性/失败保护验证，没有新增吞吐或集群性能结论 |

业务代码仍是Java/Spring Boot和Java/Spark，页面Vue3、存储MySQL8.4。Python是离线容器驱动、证据核验与测试工具；实际Parquet读取、转换及质量检测由Java/Spark执行。

## 本阶段交付与类型边界

新Java入口`cn.sparkdq.engine.ParquetImport`支持byte/short/int/long、decimal（≤38位）、boolean、受限date和严格UTF-8字符串。保留整数边界、精确小数scale、空值/空字符串/纯空格、Unicode、引号、逗号、LF换行和重复数量。日期支持1582-10-15..9999-12-31，用Spark文本+LocalDate校验。

明确拒绝浮点、时间戳、二进制/数组/映射/结构等复杂或有歧义类型，非法UTF-8、NUL、非空的字面值\N和包含CR的文本。现有csv-v2会将单元格CRLF回读为LF，因此本阶段保留历史语义，并在原生导入前拒绝此类值；不会静默改值。后续如提供类型/文本规范化策略，必须显式选择并版本化。

单个原文件12字节..20MiB、单CSV输出≤20MiB、最多100列/100万记录，单元格≤65536 UTF-16单元。源SHA必须预先固定，冻结副本只读，原父目录不挂载。Java输出后用原CSV读取器双向exceptAll核对规范字符串多重集，写manifest；宿主再核验指纹/字段/记录数并写COMPLETE。没有COMPLETE不能当作完整导入，失败诊断保留且不覆盖既有目录。

工具`infrastructure/native_parquet.py prepare/verify`采用无网络、只读系统/输入、仅新输出目录可写的容器；2CPU、2GiB内存、1GiB堆、local[2]、300秒worker预算和额外限时清理。要求宿主实际承载WSL的盘和Linux各至少5GiB余量。未提供全局配额、异步导入队列、幂等键或自动清理。

## 本阶段真实验证

| 项目 | 实际结果 |
| --- | --- |
| 构建与单元测试 | Maven package、候选镜像构建成功；86项Java测试，失败0/错误0/跳过0；75项Python工具测试通过 |
| 原生正确性 | 6行、9列真实有类型Parquet，64位极值、38位小数、日期边界、布尔/空值及特殊文本与独立Python预期逐值/多重集一致 |
| 实际工件 | 原Parquet 2592字节；规范CSV 556字节；来源/清单/规范CSV指纹均核对 |
| 失败保护 | 15类实际拒绝：空数据、空值标记冲突、NUL、超长单元格、浮点、时间戳、嵌套结构、不合法字段、输出膨胀、非法UTF-8、早期日期、CRLF、CR、错误SHA、错误文件魔数；均无成功manifest/COMPLETE |
| 压缩膨胀 | 仅3407字节的Parquet展开后会超过20MiB；实际触发写出限额，没有发布成功 |
| 公共工具入口 | 实际运行prepare、verify通过；错误SHA在写目录/启动Docker前拒绝；测试夹具不进入业务JAR |
| 真实业务闭环 | 全新恢复卷中4项Spark任务通过：转换CSV注册与7规则检测、Parquet执行副本检测、精确数值优先治理生成4行新版本、沿用原规则与格式复检；实际治理CSV与独立预期一致 |
| 兼容及持久化 | 原V4多CSV验收夹具/来源/报告保持；新任务/版本/报告跨同容器重启一致；隔离离线巡检通过，服务已停止 |
| 主环境保护 | 本阶段没有新增主库记录或替换服务镜像；所有业务行/主文件及镜像前后指纹一致，仍为223数据集/288任务、schemaVersion=4、健康UP、维护关闭 |

四任务验收通过现有CSV注册接口接收已转换CSV，未把原Parquet来源伪装为数据库已注册功能。原Parquet、转换清单及规范CSV当前在离线工件中保存；数据库原生来源引用、导入任务与备份扩展仍待下一阶段实现。

## 修复与真实失败记录

1. 首轮日期测试揭示java.sql.Date会调整1582年历法切换区间，改为Spark日期文本与LocalDate严格校验，保留原失败日志。
2. 首轮真实有类型样本被双向回读拒绝。合成诊断精确显示原CRLF变成LF；新增CR/CRLF提前拒绝和真实失败夹具，保留csv-v2行为。修复后LF样本与其余标量全部回读一致。
3. 隔离业务四任务已通过后，验收辅助程序尝试保存audit.json时与恢复工具已有审计重名。未覆盖原文件，改用post-api-audit.json，复用原4任务补完独立巡检和重启核验。该异常属于证据保存工具，未注入业务损坏或修改主环境。

## 文件与复现

源码在WSL SparkDQ-Ubuntu：`/home/sparkdq/projects/spark-dq`。新增ParquetImport.java、ParquetImportTest.java、NativeParquetFixtures.java、native_parquet.py、native_parquet_acceptance.py、native_parquet_e2e.py、test_native_parquet.py；prepared_inputs.py新增严格选择原生导入worker的分支，原有检测/准备默认行为保持。README、roadmap和本阶段说明同步。

证据根目录：`.runtime/native-parquet-154a4769c073`；有效离线工件在`cases/typed`；公开CLI复测工件在`public-cli-import`；隔离业务证据`pipeline-verified.json`，4任务夹具`.runtime/native-api-fixture-e084fb558ed3.json`。初次CRLF失败及合成诊断另留`.runtime/native-parquet-3e363e5a2a96`。均为合成数据，不自动删除。

只读核验：

```bash
timeout 90s python3 infrastructure/native_parquet.py verify /home/sparkdq/projects/spark-dq/.runtime/native-parquet-154a4769c073/cases/typed
timeout 90s python3 tests/native_parquet_acceptance.py --check /home/sparkdq/projects/spark-dq/.runtime/native-parquet-154a4769c073
```

新一轮真实格式验收与容量参数、原生转换命令、类型表见`docs/native-parquet.md`。四任务验收必须显式指定隔离API地址。源文件/规范CSV/清单和失败日志作为独立样本包交付，不包含私有SQL、用户文件或.env。

## 当前运行与下一阶段

主页面仍为http://localhost:15173/，保留原生CSV及多CSV注册和执行副本功能。新转换器只在独立worker/隔离恢复环境验证，未把候选镜像替换到主服务。没有远程推送或对外发布。

下一阶段接入有状态原生导入：持久化来源/幂等键、受控队列与容量、超时/取消/崩溃恢复、完成后原子注册、数据库原Parquet来源和备份校验，以及页面入口。之后再扩展明确的类型策略、共享存储/集群提交和更大样本。

目前仍有20MiB/100列、单后端串行任务限制，未做集群/TB级验证、完整权限/多租户、PDF/Word报告；同机备份不等于异机灾备。新运行会话北京时间2026-10-19 23:45:23到期，按既有排空规则最迟2026-10-20 00:35:23结束；保留容器和数据，不是永久自启。用户现在无需配置、认证或执行命令。
