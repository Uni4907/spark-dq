# 业务去重保留优先级（策略 v4）

成功检测任务勾选“按本任务业务键去重”后，可添加最多 5 级保留顺序，按第1、第2……优先级选择每个业务键组的第一条。页面支持调整优先级顺序、删除某级，以及选择比较方式、方向和空值位置。

固定顺序仍为标准化 → 值映射 → 填充 → 移除仍缺失记录 → 去重 → 独立CSV → 读取实际文件复检。排序使用治理后的字段值；数值/日期转换只产生Spark排序表达式，不改写保存的原字段文本。

## 接口

POST /api/tasks/{id}/govern：

~~~json
{
  "deduplicate": true,
  "policyVersion": 4,
  "deduplicationOrder": [
    {"column": "updated_day", "type": "DATE", "direction": "DESC", "nulls": "LAST"},
    {"column": "score", "type": "DECIMAL", "direction": "DESC", "nulls": "LAST"}
  ]
}
~~~

规则必须含DUPLICATE，字段必须存在且每级字段不能重复。每项只接受字符串 column/type/direction/nulls，四项均必填；方向 ASC/DESC，空值位置 FIRST/LAST。非法定义在创建任务前400拒绝。配置顺序必须显式启用deduplicate；省略policyVersion时自动规范化为4，显式2/3不能携带非空顺序。

列表顺序保留并不可变，不按字段名称重新排序。未配置顺序时仍使用原字典序，空deduplicationOrder不进入旧JSON快照；无映射默认v2、有映射默认v3、有保留顺序默认v4，历史v2/v3策略、审计与旧构造调用兼容。v4可与v3值映射组合，本轮没有数据库迁移。

## 比较方式与缺失值

| type | 比较口径 |
| --- | --- |
| TEXT | 原始文本字典序；区分大小写和空格，不自动trim或转大小写 |
| DECIMAL | decimal(38,10)，最多28位整数/10位小数，允许正负号和小数；不用double排序，不允许科学计数法或首尾空格 |
| DATE | 严格YYYY-MM-DD形状且可转换的真实日历日期；不是时间戳排序 |

NULL、空字符串、仅普通空格的值在排序表达式中统一作为缺失值，由FIRST/LAST确定位置。其他字段值及原始空白文本仍保留；如果需要先trim，显式配置标准化。只含tab等其他字符不是这里的普通空格缺失值。

对DECIMAL/DATE，移除阶段之后、去重之前的候选数据中，只要存在非空而不合法的排序值，整个治理任务明确FAILED，不产生新版本、部分SQL结果或虚构审计，不把无效值悄悄当空值。可先用标准化、值映射或填充修正，再创建新的治理任务；被移除阶段排除的记录不再参与排序校验。即使无效值所在业务键没有重复，也执行同样的全候选集校验。

所有优先级并列时，按全部字段的原文本字典序升序保留，NULL在前。完全相同的记录内容不可区分，保证保留值相同；不承诺输入CSV行位置或来源时间。未配置优先级也沿用这一历史行为。

## 审计、报告与复检

governance.policy冻结deduplicationOrder；governance.audit新增可选deduplication：

~~~json
{
  "order": [{"column":"score","type":"DECIMAL","direction":"DESC","nulls":"LAST"}],
  "tieBreak": "FULL_ROW_LEXICOGRAPHIC_ASC_NULLS_FIRST",
  "inputRows": 13,
  "duplicateGroups": 5
}
~~~

inputRows是完成变换与移除后的去重输入行数，duplicateGroups是其中出现多次的业务键组数，包含复合键和NULL键。既有deduplicatedRows继续记录移除的多余行数，transformations不把排序伪装成字段值改变。

页面策略摘要和审计展示类型/方向/空值位置及并列规则；JSON报告包含相同快照。生成独立版本后复检使用本任务规则，重复异常应变为0；“按原规则复检”只对已生成版本重新检测，不重复执行治理。旧任务没有这些审计字段时不编造历史信息。

## 验收

~~~bash
timeout 1200 python3 tests/retention_order.py
timeout 120 python3 tests/retention_order.py --check .runtime/retention-具体ID.json
~~~

--base可指向隔离副本。验收只新增合成数据和版本，保留历史记录；覆盖文本/数值差异、超过double精度的十位小数、日期、多级顺序、ASC/DESC与空值FIRST/LAST、NULL业务键、并列、字典序旧策略、变换/移除顺序、无效值失败保护、修复后治理、空输出、幂等、复检、报告与重启持久化。

仍限单后端、单机Spark local[2]、20MiB上传和串行任务；不宣称已完成集群或大规模性能实验。
