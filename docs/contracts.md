# 资料与既有接口约定

aggregate.py 已提供单分组集的整行慢路径。COUNT(*)计全部行，COUNT(x)不计null，SUM/AVG全null为null；AVG用精确有理数。COUNT DISTINCT(x,y)仅统计两参数均非null的不同有类型元组，COUNT DISTINCT(x)不计null；SUM DISTINCT只对非null值去重。GROUPING位顺序按查询维度从高位到低位，省略维度为1。重复分组集不去重，各有instance_id；空输入的空分组集输出一行，非空分组集零行。

所有资料均为合成，字段、身份及状态按本文解释。源码中的 inspect 入口读取本地资料并逐层核对分组集查询，不建立仓库管理流程。

## 分组集执行层

`aggregate.execute(rows, grouping_sets, aggregates, dimensions=None, budget=100000, spill_dir=None)` 在一次输入扫描内展开全部分组集，返回 `dimensions`、`plan`、`rows`、`source_rows` 与 `metrics`。`group_sets(...)` 只返回结果行（`details=True` 时返回完整结果），`grouping_sets` 与 `aggregate` 是同一执行入口的别名。`plan(sets, dimensions=None)` 只返回语义计划。

* 查询维度取各分组集首次出现顺序的并集；也可用 `dimensions` 显式给出从高到低的顺序。分组集含查询维度以外的名字时报错。
* `plan` 中每个实例有 `instance_id`（按写出顺序，从 0 起）、`dimensions` 与 `grouping` 位。重复写出的分组集各得一个实例，结果行不合并；每行都带 `instance_id` 和 `grouping`。
* 汇总省略的维度在输出中为 `None`，由 `grouping` 位说明来源。真实 `None` 仍是类型化 `null` 键，不使用字符串哨兵，因此“真实空团队”与“汇总占位”不会混在一起。
* 聚合描述沿用四元组 `(name, op, fields, distinct)`，也可用 `{'name','op','fields','distinct'}` 字典。`op` 支持 `count_all`、`count`、`sum`、`avg`、`min`、`max`，以及 `grouping`/`grouping_id` 标记列。`grouping` 单维度给出 0/1，多维度按位串给出整数；`grouping_id` 始终给出整数。
* 每个去重描述符拥有独立状态：同一查询内 `COUNT DISTINCT(user)`、`COUNT DISTINCT(user, plan)` 等参数不同的聚合互不串扰。非去重聚合逐行累计，保留源查询的袋语义。
* 空值约定与既有慢路径一致：COUNT(x) 不计 null，COUNT DISTINCT(x[,y]) 只计参数全部非 null 的类型化元组，SUM/AVG 全 null 为 null，SUM DISTINCT 只对非 null 值去重，AVG 用精确有理数。空输入时空分组集输出一行零计数/空值，非空分组集零行。
* 去重状态共享 `budget` 个总条目：插入前若已达预算，所有活跃状态整体写成按状态编号有序的溢写段并清空，收尾时一次归并去重求和。因此单个超大组也不能突破预算；`budget=None` 表示不溢写。`spill_dir` 给出溢写目录（默认系统临时目录），收尾后临时文件删除。
* `metrics` 报告实际状态访问量：`scans`、`source_rows`、`instances`、`groups`、`group_updates`、`aggregate_updates`、`distinct_probes`、`distinct_inserts`、`state_reads`、`state_writes`、`spills`、`spilled_entries`、`merged_entries`、`distinct_entries`、`peak_distinct_entries`、`spill_runs` 与 `budget`。
* `reference(rows, sets, aggregates, dimensions=None)` 对每个分组集单独筛行计算，不复用执行层状态；`verify(...)` 逐实例把 `execute` 结果与该独立基准比对，返回每层 `ok`。

原有 `group(rows, dimensions, aggregates)` 三参数调用保持旧的整行慢路径与返回形状（不含 `instance_id`/`grouping`）；把维度位置写成“列表的列表”或传入 `grouping_sets=` 会转入分组集执行层。
