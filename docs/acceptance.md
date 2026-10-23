# MVP 实际验收记录

日期：2026-10-16（Asia/Shanghai）。测试环境：SparkDQ-Ubuntu / Ubuntu 24.04.5，WSL2，约 11 GiB RAM + 4 GiB swap。原新项目目录为空；未修改 Windows 旧 Java、Spark、MySQL，未创建远程仓库。管理员权限仅用于用户明确授权的项目 Docker 操作。

## 最终运行版本

| 组件 | 固定版本 |
| --- | --- |
| Java / Maven | Temurin 17.0.18 / 3.9.12 |
| Spring Boot | 3.5.16 |
| Spark / Scala | 3.5.8 / 2.12.18，spark-sql_2.12 |
| MySQL Server / Connector/J | 8.4.8 LTS / 8.4.0 |
| Node / Vue / Vite / ECharts | 22.22.0 / 3.5.44 / 7.3.7 / 5.6.0 |

镜像摘要已写入 Dockerfile 和 Compose；npm package-lock.json 已生成。官方兼容性、补丁选择和依赖告警详见 versions.md。

## 实测结果

| 检查 | 结果 |
| --- | --- |
| 父 Maven 工程、backend、spark-engine 编译打包 | 成功；首轮冷缓存 7 分 29 秒，后续缓存复用 |
| GET /api/health | UP，MySQL 8.4.8 |
| 上传非法输入 | 空文件、重复/保留表头、字段数不符、无数据、未闭合引号、非法 UTF-8、非 CSV 扩展名均返回 400 |
| 20 MiB 上限 | 超大 multipart 文件返回 413 |
| RFC4180 / UTF-8 | BOM、引号中换行、大小写字段、空白物理行通过；注册与 Spark 行数一致 |
| 字段 / 业务键参数 | 空集合、重复字段、未知字段均返回 400 |
| 合成 7 行数据 | name：null 1、empty 1、纯空格 1、合计 3；email：null 2；id 重复：1 组、2 行、1 多余 |
| 复合键 / 空键 | 6 行样本按 id+region：2 组、4 行、2 多余，空键正常分组 |
| 无异常数据 | 全部指标 0、异常预览为空 |
| 保留边界 | count 业务键、id/ID 两字段、空白物理行均完成真实 Spark 回归 |
| 真实 Spark | 独立 Java 子进程，SparkSession / DataFrame / groupBy / Window / DataFrameWriter；结果记录 sparkVersion=3.5.8 |
| 文件不可读失败 | 暂时重命名合成输入再恢复；FAILED、实际 Spark PATH_NOT_FOUND 原因、执行耗时均保存；SQL 无部分成功结果 |
| SIGKILL + Compose down/up | 原有数据集、成功任务、规则、结果、异常预览保留；原文件 SHA-256 不变；遗留排队/运行任务明确 FAILED |
| 正常停止 / 重启 | 计算子进程终止；任务保留明确“中断任务”原因和耗时；服务恢复正常 |
| 环境文件初始化 | 随机密码分两行、权限 600、第二次运行不覆盖原字节 |
| 一万条合成记录 | 缺失 email 100、重复组 5000、涉及行 10000、多余行 5000；本次 7921 ms，属于本地功能烟测而非正式吞吐基准 |
| 前端 | npm ci / Vite build 成功；真实浏览器上传 → 自动注册与字段选择 → 提交任务 → 状态自动更新 → 图表和异常预览均通过 |

浏览器最终完整流程任务：18a721cf-c210-4648-bb0f-28ab3a46915e，7 行，7734 ms（页面显示 7.73 s）；自动 API 验收样本任务 a5b2281f-91f2-4c3b-9fd4-46070635e8b2，7555 ms。一万条烟测任务 67352a39-ff7c-4346-a8e7-ae39e2ef1daa。时长仅代表这台机器这次执行。

验收期间故意失败及中断的任务保留在本地数据库，属于测试记录。最初边界失败和修复经过见 development.md，不清除历史以掩盖失败。

## 可复现命令

```bash
python3 tests/e2e.py --uploads-only
python3 tests/e2e.py
python3 tests/edgecases.py
python3 tests/operational.py
# 下面两项需要项目 Docker 操作权限，会停止/重建本项目服务，勿在存在用户任务时执行
python3 tests/lifecycle.py
python3 tests/graceful_stop.py
```

详细运行与停止见 README。API 测试直接访问真实运行服务，无 mock、无 H2 替代 MySQL，也不以 Java 集合模拟 Spark。

## 完成清单

- [x] 环境、空目录、磁盘核对及官方版本兼容性检查。
- [x] 独立本地 Git、约定目录及 Maven 父工程。
- [x] MySQL 持久化，项目文件卷，随机本地密码与忽略规则。
- [x] CSV 校验、注册、字段与原始预览。
- [x] 独立 Spark JVM / local[2] / DataFrame 实算。
- [x] 可配置字段和复合业务键，空值/空字符串/空格/重复口径文档。
- [x] 数据集、任务、规则、结果独立 UUID，状态、原因、统计、耗时和路径。
- [x] Vue 上传、任务列表、详情、ECharts 图表和异常预览。
- [x] 小型已知样本、边界、失败、持久化及浏览器验收。
- [x] README、环境示例、开发记录、分阶段本地提交，无远程仓库。

## 限制与后续

- 当前 20 MiB 上传限制、单后端实例、单计算任务并发、最多 8 个等待任务、300 秒超时；未做 TB 级或分布式性能验证。
- 页面由 Vite preview 提供；后续正式部署使用静态资源服务器，并补充部署所需的认证、HTTPS、访问控制。
- ECharts 5.6.0 依照基线保留 1 个 moderate 告警；当前柱状图、非 HTML tooltip 用法规避已描述触发路径，但底层依赖尚未升级修复。
- 后续优先：数据库迁移和分页、备份/恢复工具、文件元数据对账、任务取消、更多规则与可追溯人工清洗版本；再升级 spark-submit、共享存储、集群调度并开展规模基准。
- 当前建表脚本只做 create-if-not-exists；不支持任意表结构升级。未引入 Hive/HDFS/Redis/Kafka/微服务/复杂权限。
