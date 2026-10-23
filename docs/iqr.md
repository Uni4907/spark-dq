# B1：精确十进制 IQR 检测

日期：2026-10-21。基线61ee993；本批新增IQR规则，未增加数据库迁移，未切换主部署。

## 契约

规则`{"type":"IQR","columns":["value"],"params":{"k":"1.5"},"definitionVersion":1}`。单字段，k默认1.5，(0,10]，最多4位小数。规则模板、任务快照、复检沿用冻结参数。

有效输入为无前后空格/科学计数法的十进制，最多28位整数、10位小数（与现有DECIMAL词法域一致）。空值/空串/普通纯空格、非数值、词法数值但超精度域分别计数并跳过，不能误认为这些值正常。`checkedCount`是有效值数，`evaluatedCount`是参与异常判定数。

至少4个有效值；不足时status=INSUFFICIENT_DATA，阈值为null，无异常输出，但不作“全部正常”结论。有效值分布式decimal(38,10)排序，JavaRDD zipWithIndex后仅收至多4个秩值；Q1/Q3按(n-1)p线性插值，BigDecimal精确计算IQR、k和上下界。判定用BigDecimal严格比较，等于边界正常。IQR=0报告ZERO_IQR，仍将与该常量不同的值按严格界限判异常。未把Spark double percentile冒称精确十进制，也未将全表collect到Driver。

指标算法版本iqr-exact-decimal-linear-v1；invalidCount兼容既有图表/比较，含义是有效数值中越过严格围栏的行数。报告含Q1/Q3/IQR/k/上下界、有效/判定/各类跳过与状态。每次检测/治理复检重新拟合阈值，前后指标完整保留；异常减少不等于修复。IQR不自动裁剪/删除/替换任何值。

## 本批实际验证

161项Java回归通过；17项Node测试通过，前后端构建成功。独立标准库Decimal oracle不读取Spark阈值。

隔离HTTP三任务：模板检测、模板修改后沿原规则复检、新数据复检。十类字段覆盖双侧异常、边界相等、零IQR、常量、大整数超过double精确域、不足4样本、无有效值/非法/超域、负数、小数最小尺度、28+10精度边界；全量指标和异常行多重集吻合。旧五规则的指标有独立显式断言。HTML/JSON包含精确阈值，报告比较可用，修改模板不影响历史。服务端原CSV前后下载字节和SHA均与上传一致。

另通过实际浏览器修改k、保存新模板并提交，终态及精确结果另存browser.json/截图。不可将API通过称作浏览器上传通过。

证据：`.runtime/b1-iqr-http-b`；隔离环境`.runtime/b1-iqr-20261021-b`；复现入口`tests/iqr_acceptance.py --base <隔离API> --output <新目录>`。oracle在tests/algorithm_oracle.py。输出目录必须全新；保留原始下载、输入、任务JSON、报告、摘要和COMPLETE。

## 失败记录与边界

首次Maven启动的挂载路径被宿主命令转换导致/src无pom；改用subprocess绝对参数后构建通过。首个隔离Compose生成器误替换依赖层级导致YAML错误，第二次遇Docker默认网段池耗尽；保留失败目录，改用仅服务层替换和既有网段冲突检查后在新目录启动。工具间WSL退出导致隔离服务停机，增加有界WSL会话并复用同卷恢复，保留ConnectionRefused记录；失败试次没有COMPLETE。

本批仅小样本正确性和接口贯通，不提供性能/precision/recall结论。FD、MySQL业务源、Hive待后续批次，原MySQL只是元数据库。本批未触碰主服务/旧卷、备份或历史实验，不推送、不采购云、不发布。
