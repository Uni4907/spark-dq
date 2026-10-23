# 受控CSV预置登记

默认禁用。将可信本地CSV只读挂载为`/run/controlled-inputs/example.csv`，配置文件位于数据卷外，通过`SPARK_DQ_CONTROLLED_FILES_FILE`指定，例如：

```json
{"files":[{"id":"example","name":"example.csv","path":"/run/controlled-inputs/example.csv","sha256":"替换为实际文件的64位小写SHA256"}]}
```

最多16个别名，配置最多64KiB，文件最多64MiB/100万行；必须普通文件、无路径链接、应用不可写。文件名使用ASCII字母数字、下划线、点和短横线，并以小写.csv结束。CSV沿用UTF-8/RFC4180和csv-v2原文语义，\N是NULL标记，不提供转义成普通文本的另一层语义。

GET `/api/controlled-files`只公开别名、名称、指纹和容量上限；POST `/api/controlled-files/{id}/register`不接收文件正文或路径。前端“预置大文件”可直接使用。相同别名+指纹复用已登记快照；若服务端配置变更为新指纹并重启，将登记新版本，旧数据不覆盖。同步登记期间并发请求返回429；响应不确定可重试原别名。普通上传不因此提高上限。

快照的source_manifest_json记录CONTROLLED_CSV、CONTROLLED_64M_V1、PRESET来源或GOVERNED父版本，以及实际行数/大小/完整指纹。治理可因为文件膨胀超过64MiB而失败，不发布半成品版本。源文件不是远程可选路径，客户端不能自行设置档位。治理后仍使用同档位；恢复后不需要原预置配置即可下载与复检。细节和已验收范围见[current-status-t7-2026-10-21.md](current-status-t7-2026-10-21.md)。
