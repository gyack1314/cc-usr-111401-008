# 额度仓分组集资料工具

Python 3.11 项目，用于读取、保存及核对协作资料仓的本地资料。不可变内容通过 SHA256 寻址，目录引用单独保存。现有领域接口位于 warehouse/aggregate.py，资料契约见 docs/contracts.md，合成输入位于 data/source.json。warehouse/objects.py 负责内容校验，catalog.py 负责本地引用更新，snapshot.py 提供完整行快照基础读取。

已有读取流程使用临时目录，不改变源资料。聚合执行层支持 `GROUPING SETS` 展开与 `GROUPING` 标记：`execute`/`group_sets` 在一次扫描内计算多套分组集，重复写出的分组集各保留实例，去重聚合按描述符隔离状态，超过去重条目预算时溢写到临时目录并在收尾时归并。`verify` 会逐层与独立直接计算核对并给出实际状态访问量。详细约定见 docs/contracts.md。

运行与开发检查：

```sh
python -m unittest discover -s tests -v
python -m compileall -q warehouse tests
python -m warehouse
```
