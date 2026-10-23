# 开发日志：隔离集群部署与浏览器验收（2026-10-20）

## 本阶段实际范围

以 a8c6838 为起点，承接受控同机 HTTP 集群执行。增加可重复运行的隔离部署模板、有限会话与故障工具、全局准入状态接口和任务执行面板；完成浏览器检测→治理→版本→复检→报告验证。主环境保持原有 local 模式、V5和镜像。

## 改动文件

- `infrastructure/cluster-lab.compose.yaml`：6服务、独立卷、私有计算网络及仅回环发布的edge网络，限制资源并锁定镜像。
- `infrastructure/cluster_lab.py`：create/serve/status/stop/outage，文件摘要/标签归属校验、私有凭证、PID防误杀、期限与空闲准入。
- `infrastructure/cluster-worker.Dockerfile`：预建归属UID1000的 `/work`，支持受限Worker工作卷。
- `backend/.../ExecutionStatusApi.java`：不含敏感配置的mode/pending/admission接口；同名测试覆盖空闲、停机、恢复门禁与字段范围。
- `backend/.../ClusterExecution.java`及测试：当前失败核对优先于历史CLEAR。
- `frontend/src/ExecutionPanel.vue`、`execution-view.mjs`、`App.vue`及样式：集群状态、取消等待提示、追溯折叠和准入按钮。
- `frontend/src/AnomalyChart.vue`、`chart-controller.mjs`：组件内管理图表实例与销毁，移除App跨异步流程的共享图表。
- `frontend/tests/`：5项状态语义、3项图表生命周期回归；`tests/test_cluster_lab.py`：8项控制文件、容量、归属、PID、环境覆盖、故障恢复测试。
- README、路线、开发总日志及本阶段三份文档。

完整路径和提交信息见本地交付 receipt。未增加业务Python服务；Python仍只用于运维及验收。

## 真实浏览器与独立结果核验

实验B：`spark-dq-lab-18bc186771a1`。两个合成/已知数据样本通过API注册，浏览器从选择已注册CSV开始；本轮没有冒称完成浏览器文件上传验收。

| 步骤 | 任务 | 实测 |
| --- | --- | --- |
| 检测 | 867b216a-4cb8-4e8c-919e-ea60199c9d6c | 7行，Parquet执行副本；name缺失3、email缺失2、id重复涉及2行 |
| 治理 | 48e3e99f-2252-4e9f-bfea-caba839e9213 | 填充name=UNKNOWN、email=unknown@example.test；业务键去重，7→6行 |
| 复检 | 8aa73346-8f87-46a2-93f9-145aa13825a8 | 6行，相同规则与输入方式，三项异常均为0 |
| 断联取消复测 | ddca17ca-1820-447c-8150-d5977cd649c6 | Master暂停60秒；取消保持待核对，恢复后52.31秒进入CANCELLED/CLEAR |

前三项来自真实浏览器提交，不是API代提交；成功任务有真实Spark applicationId。独立CSV预期逐行计算填充、去重结果，与下载文件完整多重集一致；原始7行文件字节保持。报告7→6、三项前后对比及attachment响应头验证通过。网页下载链接被点击，但浏览器下载落盘路径未确认；交付报告和CSV来自独立API下载，不混淆两者。

断联期间浏览器确认“暂时无法确认资源状态/正在停止”，开始检测按钮disabled；恢复后按钮恢复、cleanupPending=0、acceptingTasks=true，Master已unpause。此故障位于提交/准备阶段，不把它当作已分配Executor的中断证据。前阶段已有运行中Driver崩溃/真实超时验收，本轮不重复宣称重做这些测试。

## 遇到的问题与解决

1. Docker地址池不足：只回收本聊天历史、自有标签匹配且没有容器连接的三个空网络；容器和数据卷均保留，回执位于`.runtime/cluster-ui-network-reclaim*.json`。没有全局prune。
2. 实验A只有internal网络，未取得回环发布地址，启动健康检查失败并停止自己的容器。修正为私有计算网络+后端/前端edge网络，实验B六服务的实际挂载、网络、发布IP均核验。A的空库卷和日志保留。
3. 首轮浏览器切换出现 `Cannot read properties of undefined (reading 'all')`。拆分图表生命周期并增加针对卸载/切换的测试；修复后构建镜像并部署，重做检测/治理/复检/取消切换及断联取消，页面无错误，浏览器error/warn记录为空。未取得旧错误完整堆栈，不夸称证明了ECharts内部根因。
4. 额度中断期间演示和主会话按期限自动停止，数据未删除。恢复时先确认lab已停止，将修复后的frontend镜像SHA固定到同一lab控制文件，旧.env及元数据私有保留；随后从同一卷重启。历史任务可浏览、报告JSON内容完全一致、治理CSV字节一致。报告JSON原始字节顺序在重启后不同，因此按完整JSON内容比较；主恢复点排除采集时间后逐字段一致。这两次过严的初始验收断言已纠正，未隐藏为一次通过。

## 测试与保留证据

- Maven全量130项Java测试：0失败/错误/跳过，实际构建日志`.runtime/cluster-ui-maven.log`，Surefire XML核对130。
- Python运维全量136项通过，最终重跑日志`.runtime/cluster-ui-20261020-b/python-final.log`。
- Node8项通过，日志`.runtime/cluster-ui-node-tests-2.log`；修复后的frontend Docker构建成功，日志`cluster-ui-frontend-build-2.log`。backend/worker候选镜像均构建成功。
- 真实浏览器截图：pending、recovered、final；浏览器修复后错误/警告为空。`.runtime/cluster-ui-20261020-b/browser-verified.json`、`final-acceptance.json`保留断言结果。
- 主库225数据集/292任务/1导入，主业务恢复点SHA仍为 `1c2caca9b8dff53db7627c96c051d4cd7aa665fc3f9fb71961daa2fded3bb0c6`；源文件快照、所有业务表/结构摘要及主镜像均未变化。

不包含跨物理机、生产安全认证/HA、更大规模集群性能、公平local/cluster速度对照。后台服务期限和交付地址以完整进度为准。
