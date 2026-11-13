# 资料与既有接口约定

aggregate.py 已提供单分组集的整行慢路径。COUNT(*)计全部行，COUNT(x)不计null，SUM/AVG全null为null；AVG用精确有理数。COUNT DISTINCT(x,y)仅统计两参数均非null的不同有类型元组，COUNT DISTINCT(x)不计null；SUM DISTINCT只对非null值去重。GROUPING位顺序按查询维度从高位到低位，省略维度为1。重复分组集不去重，各有instance_id；空输入的空分组集输出一行，非空分组集零行。

所有资料均为合成，字段、身份及状态按本文解释。源码中的 inspect 入口只读取并核对现有基础接口；不会建立高级查询或仓库管理流程。
