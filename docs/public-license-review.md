# 公开发布许可核查（2026-10-23）

状态：PARTIAL（传递依赖人工法律兼容审查未穷尽）。作者于2026-10-23明确确认公开与许可授权、学校规定无冲突，并同意采用Apache-2.0；已添加根目录LICENSE。

## 自有源码

依据作者确认，为项目自有源码添加Apache-2.0标准全文。允许学习、修改、再分发和商业使用，要求保留许可与适用通知，包含专利授权条款。该许可不替换第三方组件许可；Connector/J的GPLv2及FOSS例外见下。

官方文本：https://www.apache.org/licenses/LICENSE-2.0

## 第三方检查

- Spark 3.5.8：Apache-2.0；官方版本许可 https://raw.githubusercontent.com/apache/spark/v3.5.8/LICENSE 。分发其组件时保留随附 LICENSE/NOTICE 及第三方通知。
- Vue 3.5.44：MIT；官方版本许可 https://raw.githubusercontent.com/vuejs/core/v3.5.44/LICENSE 。保留版权与许可声明。
- Connector/J 8.4.0：GPLv2，并有额外链接许可及 Universal FOSS Exception 1.0。不能把它直接标为 Apache/MIT；应保留原许可，并按最终项目许可与分发方式核对例外适用条件。官方版本材料 https://raw.githubusercontent.com/mysql/mysql-connector-j/8.4.0/LICENSE 。
- 前端锁文件88个包均有许可声明，声明类别为0BSD、Apache-2.0、BSD-2-Clause、BSD-3-Clause、ISC、MIT；不能只检查顶层Vue。构建产物保留工具生成的许可文件，不移除依赖包内的 LICENSE/NOTICE。
- Java 传递依赖的许可材料应随实际重新获取的制品保留。当前不发布预构建 JAR 或 Docker 镜像，不代表可忽略日后分发义务。
- Docker 基础镜像及 MySQL Server 各有许可与组成软件通知，本项目许可不能替代这些许可。

未完成：全部传递依赖的人工法律兼容性审查。学校/源码公开权利及项目许可证选择已获作者明确确认；未声称独立审查了学校的正式政策原文。

## 数据

快速开始仅用 demo/orders.csv 等人工合成夹具，不下载真实业务数据。datasets/known-issues.csv 的来源说明保留。历史 UCI Bank Marketing 实验保留署名、DOI、派生说明与 CC BY 4.0 引用；本候选不提交其原始 CSV 或下载包。官方许可页：https://archive.ics.uci.edu/dataset/222/bank+marketing 。历史实验数据许可不等于项目源码许可。
