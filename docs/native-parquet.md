# 原生 Parquet 离线导入准备：v1 契约

此阶段提供Java/Spark独立转换器和受限容器工具，把一份原生有类型Parquet转换为可由现有系统注册的规范csv-v2。在线导入、持久化任务/取消恢复和原文件来源已在下一阶段接入V5，见[在线导入说明](native-imports.md)。本文描述仍可独立使用的离线CLI及其更严格的容器隔离边界。

## 类型与空值契约

| 原生读取类型 | 规范文本 | 边界 |
| --- | --- | --- |
| byte/short/int/long | 十进制整数 | 保留64位最小/最大值，不经浮点数 |
| decimal(p,s) | toPlainString，保留scale | Spark精度≤38，不转double或科学计数 |
| boolean | true / false | null仍为缺失 |
| date | YYYY-MM-DD | 1582-10-15..9999-12-31；用Spark日期文本和LocalDate严格校验，避开java.sql.Date历法切换修正 |
| string | 原始UTF-8严格解码 | 保留空文本、空格、Unicode、引号、逗号、LF换行；拒绝非法UTF-8、NUL、回车CR以及非空的字面值\N |
| null | CSV缺失标记\N | 与空字符串/纯空格分别验证 |

float/double、timestamp/timestamp_ntz、binary、array/map/struct等不自动猜测转换。timestamp时区、浮点精度、嵌套结构需要后续明确策略，当前拒绝整份文件。字段须唯一，英文字母开头，最多64字符/100列。

现有csv-v2会把单元格内CRLF回读为LF；新导入器拒绝任何含CR的原生文本，保持既有任务/历史快照的语义不变。若要规范化，须在上游明确决定，而不是让系统静默改值。原生Parquet非空字符串\N会与CSV空值标记冲突，也明确拒绝。其他读回不一致同样拒绝，不发布成功。

## 受控边界与工件

输入：单份、本机、非链接、未加密PAR1 Parquet，12字节..20MiB。必须提供预期SHA-256。工具先在全新任务目录冻结一份只读source.parquet，核对复制前后指纹；不把原父目录或其他文件挂进容器。worker只接受固定input/output/sourceSha256三个字段，不接受任意Spark选项、执行类或读取目录/通配符。

输出：独立规范CSV≤20MiB、最多100万记录，单元格≤65536个UTF-16单元；CSV按全引号RFC4180输出。Parquet行顺序不作为契约，保留完整行多重集（包括重复数量）。压缩文件大小不能替代输出限额。

Java逐行转换，再由现有Engine.readCsv读回，以两方向exceptAll、字段顺序和记录数核对规范字符串多重集；最后重新核对源SHA。通过后以原子移动发布canonical.csv和manifest.json，包含源指纹/大小、读取字段类型、转换版本、规范CSV指纹/大小、行数。CLI再独立核验CSV/元数据并写COMPLETE；外部消费者必须要求COMPLETE，不能只看目录或某个CSV存在。

失败留下原始冻结源、日志和部分临时CSV供诊断，不覆盖既有目录，也不自动清理。可能存在worker已发布manifest但外层核验/中断尚未写COMPLETE的情况，仍不能当作完整导入。没有写数据库或生产卷。

容器无网络、根文件系统只读、输入只读、仅输出任务目录可写；local[2]、2CPU、2GiB内存/无额外swap、1GiB堆、512进程、768MiB临时文件系统。worker限时300秒并有终止宽限，宿主工具有额外Docker操作限时；中断仅停止匹配本次标签的容器，保留诊断。离线CLI未实现异步队列/幂等键；在线V5提供持久化导入队列与幂等键。全局磁盘硬配额、自动清理及集群提交仍待完成。

## 操作与复现

需要本项目Docker权限；当前发行版使用root执行Docker。先按项目构建流程完成Maven package和backend.Dockerfile，标签spark-dq-backend:native-parquet。Java测试夹具仅在test-classes，不进入业务JAR。

准备host-capacity.json，包含实际承载WSL磁盘的盘符空闲容量，例如`{"known":true,"freeBytes":实际空闲字节数}`；不能把WSL虚拟磁盘宣称的容量当作宿主真实空闲。工具要求宿主和Linux均至少5GiB余量。

~~~bash
timeout 380s python3 infrastructure/native_parquet.py prepare \
  --input /absolute/data.parquet \
  --sha256 预先核对的64位小写SHA256 \
  --output .runtime/native-import-唯一目录 \
  --host-capacity /absolute/host-capacity.json
timeout 90s python3 infrastructure/native_parquet.py verify .runtime/native-import-唯一目录
~~~

prepare复制并转换本地文件；verify仅只读核对来源、元数据、规范CSV和COMPLETE。不会自动上传或注册数据集，也不会改变现有服务镜像。

真实验收（只生成合成Parquet，核对主库/文件/镜像不变）与已有证据核验：

~~~bash
timeout 1800s python3 tests/native_parquet_acceptance.py --host-capacity /absolute/host-capacity.json
timeout 90s python3 tests/native_parquet_acceptance.py --check /absolute/native-parquet-实验目录
~~~

验收生成精确标量、特殊文本及故意损坏/不支持的Parquet样本；拒绝测试检查进程日志对应具体原因，以及缺少manifest/COMPLETE，不能把任意异常都算通过。已有证据只读核验检查源文件指纹与所有结果工件，不重新解释历史主库状态。

`tests/native_parquet_e2e.py`显式指定隔离API地址，把合成转换CSV通过现有CSV接口注册，再核对真实Spark统计、Parquet执行副本、治理、独立CSV版本和复检。该验收不表示网页已接收Parquet，原Parquet来源目前保存在离线工件中。不得把测试脚本默认路由到用户主环境。

## 下一步

优先将此契约接入持久化导入任务：来源指纹、幂等键、受控队列/共享预算、取消/失败/重启恢复、完成后原子注册和数据库来源引用；同步扩展备份巡检及页面。类型策略版本必须冻结，再扩展时间戳/浮点等有歧义类型。
