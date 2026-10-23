# MySQL 业务快照导入

业务快照与系统自身的 MySQL 元数据存储分别配置。未设置 `SPARK_DQ_MYSQL_SOURCES_FILE` 时，业务源列表为空，原有功能仍可启动。此功能不运行用户提供的 SQL，不接受请求中的 JDBC URL、主机或凭证。

## 服务端配置

将私有配置和口令文件只读挂载到 `/run` 下，配置路径写入 `SPARK_DQ_MYSQL_SOURCES_FILE`。配置文件和口令文件不能位于备份数据目录 `/data`，不能使用文件符号链接。不要把这些文件提交 Git 或放进交付包。账户只授予白名单表的 SELECT 权限。

配置格式如下，示例没有可用凭证：

```json
{"sources":[{"id":"orders","host":"business-db","port":3306,"user":"snapshot_reader","tlsMode":"VERIFY_IDENTITY","passwordFile":"/run/orders.password","schema":"business","table":"orders","columns":[{"source":"订单号","target":"order_id"},{"source":"金额","target":"amount"}],"maxRows":1000000,"maxBytes":20971520,"timeoutSeconds":30}]}
```

生产连接默认校验 TLS 主机身份；`DISABLED` 仅用于受控实验网络。口令文件结尾空白会去除，因此当前配置契约不支持以空白结尾的口令。主机、库表、字段及映射均由服务端固定；调用者只能从白名单中选择字段及顺序。配置最多 16 个源，文件不超过 64KiB，口令文件不超过 4096 字节。

## 正式 API

1. `GET /api/mysql-sources` 获取表、字段映射和限额。返回内容不包含连接主机、账号、口令和私密文件路径。
2. `POST /api/mysql-imports`，请求头 `X-Import-Key` 为新 UUID，请求体 `{"sourceId":"orders","columns":["订单号","金额"]}`。返回 202 和导入记录。
3. `GET /api/mysql-imports/{id}` 轮询终态；仅 `SUCCEEDED` 才提供可用 `dataset_id`。相同标识与相同请求返回原记录，改变请求则 409。
4. `POST /api/mysql-imports/{id}/cancel` 取消排队或读取中的作业。运行中的取消先进入 `CANCELLING`，连接退出后才成为 `CANCELLED`。
5. 将所得数据集交给既有任务、规则模板、报告和原规则复检 API。数据集下载与 `/api/datasets/{id}/sources/1/download` 返回保存的快照，不重新连接业务库。

`GET /api/mysql-imports/page` 分页列出该类导入。旧 Parquet 导入 API 不操作 MySQL 作业。服务重启后，未完成的业务导入记为失败或取消；不会在同一来源记录下悄悄重读变化后的业务库。

## 数据与边界

单连接、只读 Repeatable Read 一致性事务；先锁定元数据再核验，仅接受 InnoDB 基础表。支持受约束的整数、精确 DECIMAL、DATE、VARCHAR/TEXT；拒绝浮点、CHAR 等可能丢失语义的转换。整数和小数不用 double 中转。NULL 写作 `\N`，空串、普通空格、中文、制表、引号及单元格换行按原文保存。业务字符串恰为 `\N` 时明确失败，避免与 NULL 混淆。字段内容上限 65536 个 Java 字符。

行数、20MiB 字节上限、查询时限均为硬边界，不能以静默截断取得成功。输出先在私有临时目录完成、核验和冻结，再原子发布。来源清单记录列映射、SQL 类型、事务隔离、行数、字节数和 SHA-256；不保存口令。导入与检测共用受控串行队列，同时受维护窗口和数据保留预算约束。

检测入口显式指定 CSV 记录分隔符，以保留引号内 CRLF/LF/CR。规范快照使用 LF；普通 CSV 从受约束的表头识别记录分隔符。不要混用记录分隔符；此项与单元格内同时包含多种换行不同。原文件下载仍返回保存的文件字节。

备份恢复纳入业务来源、快照和导入记录的一致性校验。恢复不携带业务源配置或口令；保存的快照下载和复检不依赖业务库在线。备份验证失败时不得宣称恢复成功。正式验收结果应以本批实际报告及原始证据为准。
