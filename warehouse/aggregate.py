"""精确的分组集聚合执行层。

既有的 ``group(rows, dimensions, aggregates)`` 保留单分组集、整行缓冲的慢
路径和原有空值约定，行为不变。新增的 ``execute``/``group_sets`` 在一次输入
扫描中展开多个分组集：

* 分组集按写出顺序展开，重复写出的分组集各自拥有 ``instance_id``，不合并；
* 汇总省略的维度在输出中为 ``None``，由每行的 ``grouping`` 位标注来源，
  真实 ``None`` 键仍以类型化 ``null`` 编码参与分组，不使用字符串哨兵；
* 每个去重聚合描述符拥有独立的去重状态，同一查询里参数不同的多个
  ``COUNT DISTINCT``/``SUM DISTINCT`` 不会互相串扰；
* 非去重聚合按行累计，保留源查询的袋语义（完全相同的行也逐行计数）；
* 所有去重状态共享一个总条目预算。插入新键之前若已达预算，就把全部活跃
  状态写成按状态编号排序的段并清空；收尾时一次归并累加，故单个超大组也
  不会突破预算。非去重的分组小状态留在内存。
"""
from fractions import Fraction
import heapq
import json
import os
import tempfile

from .typed import key

DEFAULT_BUDGET = 100_000
_RESERVED = {'instance_id', 'grouping'}
_AGG_OPS = {'count_all', 'count', 'sum', 'avg', 'min', 'max', 'grouping', 'grouping_id'}
_VALUE_OPS = {'sum', 'avg', 'min', 'max'}
_MERGE_FAN_IN = 32


def _legacy_group(rows, dimensions, aggregates):
    """原单分组集慢路径，逐行保留，语义不变。"""
    groups = {}
    for row in rows:
        values = tuple(row[d] for d in dimensions); encoded = key(values)
        groups.setdefault(encoded, [values, []])[1].append(row)
    if not dimensions and not groups: groups[key(())] = ((), [])
    result = []
    for values, members in groups.values():
        output = {d: v for d, v in zip(dimensions, values)}
        for name, op, fields, distinct in aggregates:
            if op == 'count_all': output[name] = len(members); continue
            inputs = [tuple(r[f] for f in fields) for r in members]
            inputs = [v for v in inputs if all(x is not None for x in v)]
            if distinct: inputs = list({key(v): v for v in inputs}.values())
            if op == 'count': value = len(inputs)
            elif op == 'sum': value = sum(v[0] for v in inputs) if inputs else None
            elif op == 'avg': value = str(Fraction(sum(v[0] for v in inputs), len(inputs))) if inputs else None
            else: raise ValueError('聚合不支持')
            output[name] = value
        result.append(output)
    return result


def _is_spec(value):
    """判断一个聚合描述符（四元组或字典）。"""
    if isinstance(value, dict):
        return 'name' in value and 'op' in value
    return (isinstance(value, (tuple, list)) and len(value) == 4
            and isinstance(value[0], str) and isinstance(value[1], str))


def _looks_like_sets(value):
    return (isinstance(value, (list, tuple)) and len(value) > 0
            and all(isinstance(item, (list, tuple)) for item in value))


def _normalize_specs(aggregates, query_dimensions):
    specs = []
    names = set()
    for item in aggregates:
        if not _is_spec(item):
            raise ValueError('聚合描述符必须是四元组或字典')
        if isinstance(item, dict):
            name = item.get('name'); op = item.get('op')
            fields = item.get('fields', ()); distinct = bool(item.get('distinct', False))
        else:
            name, op, fields, distinct = item
        if not isinstance(name, str): raise ValueError('聚合名称必须为字符串')
        if name in names: raise ValueError('聚合名称重复')
        if name in _RESERVED:
            raise ValueError('聚合名称与保留键冲突')
        names.add(name)
        if op not in _AGG_OPS: raise ValueError('聚合不支持')
        if isinstance(fields, str): fields = (fields,)
        fields = tuple(fields)
        if any(not isinstance(f, str) for f in fields): raise ValueError('聚合字段必须为字符串')
        if op == 'count_all':
            fields = ()
            distinct = False
        elif op in {'grouping', 'grouping_id'}:
            fields = tuple(query_dimensions) if not fields else fields
            if any(f not in query_dimensions for f in fields):
                raise ValueError('GROUPING 维度不在查询维度中')
            if distinct: raise ValueError('GROUPING 不支持去重')
        else:
            if not fields: raise ValueError('聚合缺少字段')
            if op in _VALUE_OPS and len(fields) != 1:
                raise ValueError('值聚合只接受单个字段')
            if op == 'count' and len(fields) > 1 and not distinct:
                raise ValueError('COUNT 只接受单个字段；多字段需去重')
        specs.append({'name': name, 'op': op, 'fields': fields, 'distinct': bool(distinct)})
    return specs


def grouping(query_dimensions, dimensions):
    """按查询维度顺序返回 GROUPING 位，0 表示参与分组，1 表示被省略。"""
    present = set(dimensions)
    return [0 if dimension in present else 1 for dimension in query_dimensions]


def grouping_id(query_dimensions, dimensions):
    """GROUPING 位的二进制值，第一个查询维度为最高位。"""
    bits = grouping(query_dimensions, dimensions)
    return int(''.join(str(bit) for bit in bits), 2) if bits else 0


def expand(grouping_sets, dimensions=None):
    """展开分组集并生成带实例身份的语义计划。

    重复写出的分组集不合并，各自得到一个 ``instance_id``。查询维度默认按
    各分组集首次出现的顺序取并集；也可以用 ``dimensions`` 显式给出从高位到
    低位的顺序。
    """
    if not isinstance(grouping_sets, (list, tuple)) or not grouping_sets:
        raise ValueError('分组集必须是非空列表')
    sets = []
    for raw in grouping_sets:
        if not isinstance(raw, (list, tuple)): raise ValueError('分组集必须是维度列表')
        names = tuple(raw)
        if any(not isinstance(name, str) for name in names): raise ValueError('分组维度必须为字符串')
        if len(set(names)) != len(names): raise ValueError('分组维度重复')
        sets.append(names)
    if dimensions is None:
        query_dimensions = []
        seen = set()
        for names in sets:
            for name in names:
                if name not in seen:
                    seen.add(name); query_dimensions.append(name)
        query_dimensions = tuple(query_dimensions)
    else:
        if isinstance(dimensions, str): raise ValueError('查询维度必须是列表')
        query_dimensions = tuple(dimensions)
        if any(not isinstance(name, str) for name in query_dimensions):
            raise ValueError('查询维度必须为字符串')
        if len(set(query_dimensions)) != len(query_dimensions): raise ValueError('查询维度重复')
    allowed = set(query_dimensions)
    entries = []
    for instance_id, names in enumerate(sets):
        if set(names) - allowed:
            raise ValueError('分组维度不在查询维度中')
        entries.append({
            'instance_id': instance_id,
            'dimensions': list(names),
            'grouping': grouping(query_dimensions, names),
        })
    return list(query_dimensions), entries


class _Run:
    """有序溢写段，每行 ``state_id \\t key_hex [\\t value_json]``。

    编码键本身是不含制表符的十六进制串，所以按行分段一遍归并即可按
    （状态、键）去重并累加。
    """

    __slots__ = ('path', 'level', 'owner')

    def __init__(self, path, level, owner):
        self.path = path
        self.level = level
        self.owner = owner

    def __iter__(self):
        with open(self.path, 'r', encoding='utf-8') as stream:
            for line in stream:
                parts = line.rstrip('\n').split('\t')
                yield int(parts[0]), parts[1], (json.loads(parts[2]) if len(parts) > 2 else None)

    def close(self):
        try: os.unlink(self.path)
        except FileNotFoundError: pass


def _merge(streams):
    return heapq.merge(*streams, key=lambda item: (item[0], item[1]))


class _DistinctState:
    """分组内单个去重聚合的不同键集合。state_id 是它在溢写段中的编号。"""

    __slots__ = ('mem', 'state_id', 'keep_values', 'result')

    def __init__(self, state_id, keep_values):
        self.mem = {}
        self.state_id = state_id
        self.keep_values = keep_values
        self.result = None


class _Group:
    __slots__ = ('values', 'count_all', 'non_distinct', 'distinct')

    def __init__(self, values, specs):
        self.values = values
        self.count_all = 0
        self.non_distinct = [None] * len(specs)
        self.distinct = [None] * len(specs)


class _Execution:
    """一次扫描内的全部实例状态。"""

    def __init__(self, plan, specs, query_dimensions, budget, spill_dir):
        self.plan = plan
        self.specs = specs
        self.query_dimensions = query_dimensions
        self.budget = budget
        self.spill_dir = spill_dir
        self.groups = {entry['instance_id']: {} for entry in plan}
        self._dims = {entry['instance_id']: tuple(entry['dimensions']) for entry in plan}
        self._positions = {}
        for entry in plan:
            names = self._dims[entry['instance_id']]
            self._positions[entry['instance_id']] = [
                names.index(name) if name in names else -1 for name in query_dimensions]
        self._tempdir = None
        self._distinct_entries = 0
        self._peak_entries = 0
        self._next_state_id = 0
        self._states = []
        self._active = []
        self._active_set = set()
        self._runs = []
        self.metrics = {
            'scans': 0, 'source_rows': 0, 'instances': len(plan),
            'groups': 0, 'group_updates': 0, 'aggregate_updates': 0,
            'distinct_probes': 0, 'distinct_inserts': 0,
            'state_reads': 0, 'state_writes': 0,
            'spills': 0, 'spilled_entries': 0, 'merged_entries': 0,
            'distinct_entries': 0, 'peak_distinct_entries': 0,
            'spill_runs': 0, 'budget': budget,
        }
        # 空分组集即使没有任何输入也必须产出一行；非空分组集则零行。
        for entry in plan:
            if not entry['dimensions']:
                self._group(entry['instance_id'], ())

    # -- 状态与溢写 ------------------------------------------------------

    def _new_run_path(self):
        if self._tempdir is None:
            self._tempdir = tempfile.mkdtemp(dir=self.spill_dir, prefix='.agg-spill-')
        handle, path = tempfile.mkstemp(dir=self._tempdir, prefix='.run-')
        os.close(handle)
        return path

    def _spill_all(self):
        """把全部活跃去重状态整体写成有序段并清空内存。"""
        if not self._active: return
        path = self._new_run_path()
        spilled = 0
        with open(path, 'w', encoding='utf-8') as stream:
            # 段内必须按状态编号递增写出，归并才成立。
            for state in sorted(self._active, key=lambda item: item.state_id):
                prefix = str(state.state_id) + '\t'
                items = sorted(state.mem.items())
                spilled += len(items)
                if state.keep_values:
                    for encoded_key, value in items:
                        stream.write(prefix + encoded_key.hex() + '\t'
                                     + json.dumps(value, ensure_ascii=False, allow_nan=False) + '\n')
                else:
                    for encoded_key, _ in items:
                        stream.write(prefix + encoded_key.hex() + '\n')
                state.mem.clear()
        self._active.clear()
        self._active_set.clear()
        self._runs.append(_Run(path, 0, self))
        self.metrics['spills'] += 1
        self.metrics['spilled_entries'] += spilled
        self._distinct_entries = 0
        self._compact()

    def _merge_runs(self, runs, level):
        path = self._new_run_path()
        count = 0
        with open(path, 'w', encoding='utf-8') as stream:
            for state_id, encoded_key, value in _merge(iter(run) for run in runs):
                stream.write(str(state_id) + '\t' + encoded_key)
                if value is not None:
                    stream.write('\t' + json.dumps(value, ensure_ascii=False, allow_nan=False))
                stream.write('\n')
                count += 1
        for run in runs: run.close()
        self._runs = [run for run in self._runs if run not in runs]
        merged = _Run(path, level, self)
        self._runs.append(merged)
        self.metrics['merged_entries'] += count
        return merged

    def _compact(self):
        """分层合并：任一层段数达到扇入上限就并入上一层，均摊 O(n log n)。"""
        while True:
            for level in sorted({run.level for run in self._runs}):
                same = [run for run in self._runs if run.level == level]
                if len(same) >= _MERGE_FAN_IN:
                    self._merge_runs(same[:_MERGE_FAN_IN], level + 1)
                    break
            else:
                return

    def _reduce_runs(self):
        """收尾前把剩余段按扇入上限合并，避免同时打开过多文件。"""
        while len(self._runs) > _MERGE_FAN_IN:
            level = max(run.level for run in self._runs[:_MERGE_FAN_IN])
            self._merge_runs(self._runs[:_MERGE_FAN_IN], level + 1)
        self.metrics['spill_runs'] = len(self._runs)

    def _distinct_records(self):
        """按（状态编号、键）去重有序地产出记录。"""
        self._reduce_runs()
        streams = []
        memory = []
        for state in self._states:
            for encoded_key, value in state.mem.items():
                memory.append((state.state_id, encoded_key.hex(), value))
        memory.sort(key=lambda item: (item[0], item[1]))
        if memory: streams.append(iter(memory))
        streams.extend(iter(run) for run in self._runs)
        last = object()
        for record in _merge(streams):
            marker = record[:2]
            if marker != last:
                last = marker
                yield record

    # -- 扫描 ------------------------------------------------------------

    def _group(self, instance_id, values):
        encoded = key(values)
        group = self.groups[instance_id].get(encoded)
        if group is None:
            positions = self._positions[instance_id]
            full = tuple(values[position] if position >= 0 else None for position in positions)
            group = _Group(full, self.specs)
            self.groups[instance_id][encoded] = group
            self.metrics['groups'] += 1
            for index, spec in enumerate(self.specs):
                if spec['distinct']:
                    state = _DistinctState(self._next_state_id, spec['op'] in _VALUE_OPS)
                    self._next_state_id += 1
                    group.distinct[index] = state
                    self._states.append(state)
        return group

    def scan(self, rows):
        self.metrics['scans'] += 1
        for row in rows:
            self.metrics['source_rows'] += 1
            for entry in self.plan:
                instance_id = entry['instance_id']
                dimensions = self._dims[instance_id]
                values = tuple(row[name] for name in dimensions)
                group = self._group(instance_id, values)
                group.count_all += 1
                self.metrics['group_updates'] += 1
                self.metrics['state_writes'] += 1
                for index, spec in enumerate(self.specs):
                    op = spec['op']
                    if op in {'count_all', 'grouping', 'grouping_id'}:
                        continue
                    inputs = tuple(row[name] for name in spec['fields'])
                    if any(value is None for value in inputs):
                        continue
                    if spec['distinct']:
                        encoded_key = key(inputs)
                        state = group.distinct[index]
                        self.metrics['distinct_probes'] += 1
                        if encoded_key in state.mem:
                            continue
                        if self.budget is not None and self._distinct_entries >= self.budget:
                            self._spill_all()
                        state.mem[encoded_key] = inputs[0] if state.keep_values else None
                        if state not in self._active_set:
                            self._active_set.add(state); self._active.append(state)
                        self._distinct_entries += 1
                        self.metrics['distinct_inserts'] += 1
                        self.metrics['state_writes'] += 1
                        continue
                    partial = group.non_distinct[index]
                    if op == 'count':
                        group.non_distinct[index] = (0 if partial is None else partial) + 1
                    elif op in {'sum', 'avg'}:
                        if partial is None: partial = [0, 0]
                        partial[0] += inputs[0]; partial[1] += 1
                        group.non_distinct[index] = partial
                    elif op == 'min':
                        group.non_distinct[index] = inputs[0] if partial is None else min(partial, inputs[0])
                    elif op == 'max':
                        group.non_distinct[index] = inputs[0] if partial is None else max(partial, inputs[0])
                    self.metrics['aggregate_updates'] += 1
                    self.metrics['state_writes'] += 1
            if self._distinct_entries > self._peak_entries:
                self._peak_entries = self._distinct_entries

    # -- 结果 ------------------------------------------------------------

    def _compile_distinct(self):
        """归并一遍全部去重记录，得到每个状态的结果。"""
        self._state_results = {}
        current = None
        values = []
        count = 0
        for state_id, _, value in self._distinct_records():
            self.metrics['state_reads'] += 1
            if current is not None and state_id != current:
                self._state_results[current] = self._value_result_by_id(current, values, count)
                values = []; count = 0
            current = state_id
            count += 1
            if self._state_keep[state_id]:
                values.append(value)
        if current is not None:
            self._state_results[current] = self._value_result_by_id(current, values, count)
        for state in self._states:
            if state.state_id in self._state_results:
                state.result = self._state_results[state.state_id]
            else:
                # 空分组集的状态没有任何记录：COUNT 为 0，值聚合为空。
                state.result = 0 if self._state_spec[state.state_id]['op'] == 'count' else None

    def _value_result_by_id(self, state_id, values, count):
        op = self._state_spec[state_id]['op']
        if op == 'count': return count
        if not values: return None
        if op == 'sum': return sum(values)
        if op == 'avg': return str(Fraction(sum(values), len(values)))
        if op == 'min': return min(values)
        if op == 'max': return max(values)
        raise ValueError('聚合不支持')

    def _aggregate(self, group, instance_id, index, entry):
        spec = self.specs[index]; op = spec['op']; name = spec['name']
        if op == 'count_all':
            self.metrics['state_reads'] += 1
            return name, group.count_all
        if op in {'grouping', 'grouping_id'}:
            omitted = set(self.query_dimensions) - set(entry['dimensions'])
            bits = [1 if dimension in omitted else 0 for dimension in spec['fields']]
            value = bits[0] if op == 'grouping' and len(bits) == 1 else int(''.join(map(str, bits)), 2)
            self.metrics['state_reads'] += 1
            return name, value
        if spec['distinct']:
            return name, group.distinct[index].result
        self.metrics['state_reads'] += 1
        partial = group.non_distinct[index]
        if op == 'count': return name, 0 if partial is None else partial
        if op in {'sum', 'avg'}:
            if partial is None or partial[1] == 0: return name, None
            if op == 'sum': return name, partial[0]
            return name, str(Fraction(partial[0], partial[1]))
        return name, partial

    def finish(self):
        # 预先登记每个状态对应的聚合描述，供归并累加时取值。
        self._state_spec = {}
        self._state_keep = {}
        for entry in self.plan:
            instance_id = entry['instance_id']
            for group in self.groups[instance_id].values():
                for index, state in enumerate(group.distinct):
                    if state is not None:
                        self._state_spec[state.state_id] = self.specs[index]
                        self._state_keep[state.state_id] = state.keep_values
        self._compile_distinct()
        result = []
        for entry in self.plan:
            instance_id = entry['instance_id']
            for encoded_group, group in self.groups[instance_id].items():
                output = dict(zip(self.query_dimensions, group.values))
                output['instance_id'] = instance_id
                output['grouping'] = list(entry['grouping'])
                for index, spec in enumerate(self.specs):
                    name, value = self._aggregate(group, instance_id, index, entry)
                    output[name] = value
                result.append(output)
        self.metrics['distinct_entries'] = self.metrics['distinct_inserts']
        self.metrics['peak_distinct_entries'] = max(self._peak_entries, self._distinct_entries)
        return result

    def close(self):
        for run in self._runs: run.close()
        self._runs = []
        if self._tempdir is not None:
            for name in os.listdir(self._tempdir):
                try: os.unlink(os.path.join(self._tempdir, name))
                except FileNotFoundError: pass
            try: os.rmdir(self._tempdir)
            except FileNotFoundError: pass


def _require_fields(rows, query_dimensions, specs):
    """在扫描前统一检查字段存在，避免只扫到部分行才报错。"""
    required = set(query_dimensions)
    for spec in specs:
        if spec['op'] not in {'grouping', 'grouping_id'}:
            required.update(spec['fields'])
    missing = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError('行必须是字典')
        missing.update(name for name in required if name not in row)
    if missing:
        raise ValueError('行缺少字段: ' + ', '.join(sorted(missing)))


def execute(rows, grouping_sets, aggregates, dimensions=None, budget=DEFAULT_BUDGET,
            spill_dir=None):
    """一次扫描执行分组集展开，返回语义计划、结果行和实际状态访问量。"""
    if budget is not None and (not isinstance(budget, int) or isinstance(budget, bool)
                               or budget < 1):
        raise ValueError('预算必须是正整数或 None')
    if isinstance(rows, (str, bytes, dict)):
        raise ValueError('行必须是可迭代的行序列')
    if not isinstance(rows, (list, tuple)):
        rows = list(rows)
    query_dimensions, entries = expand(grouping_sets, dimensions)
    specs = _normalize_specs(aggregates, query_dimensions)
    _require_fields(rows, query_dimensions, specs)
    execution = _Execution(entries, specs, query_dimensions, budget, spill_dir)
    try:
        execution.scan(rows)
        result_rows = execution.finish()
        return {
            'dimensions': list(query_dimensions),
            'grouping': list(query_dimensions),
            'plan': execution.plan,
            'rows': result_rows,
            'source_rows': execution.metrics['source_rows'],
            'metrics': execution.metrics,
        }
    finally:
        execution.close()


def group_sets(rows, grouping_sets, aggregates, dimensions=None,
               budget=DEFAULT_BUDGET, spill_dir=None, details=False):
    """返回展开后的结果行；``details=True`` 时返回计划与指标。"""
    result = execute(rows, grouping_sets, aggregates, dimensions, budget, spill_dir)
    return result if details else result['rows']


def aggregate(rows, grouping_sets, aggregates, dimensions=None,
              budget=DEFAULT_BUDGET, spill_dir=None):
    """``execute`` 的别名。"""
    return execute(rows, grouping_sets, aggregates, dimensions, budget, spill_dir)


def grouping_sets(rows, sets, aggregates, dimensions=None,
                  budget=DEFAULT_BUDGET, spill_dir=None):
    """``execute`` 的具名别名。"""
    return execute(rows, sets, aggregates, dimensions, budget, spill_dir)


def plan(grouping_sets, dimensions=None):
    """只返回分组集展开后的语义计划。"""
    query_dimensions, entries = expand(grouping_sets, dimensions)
    return {'dimensions': list(query_dimensions), 'plan': entries}


def reference(rows, grouping_sets, aggregates, dimensions=None):
    """不共享扫描的独立直接计算，用作核对基准。

    每个分组集单独筛行、单独分组、单独计算，不复用执行层的任何状态。
    """
    rows = list(rows)
    query_dimensions, entries = expand(grouping_sets, dimensions)
    specs = _normalize_specs(aggregates, query_dimensions)
    result = []
    for entry in entries:
        names = tuple(entry['dimensions'])
        partitions = {}
        for row in rows:
            values = tuple(row[name] for name in names)
            partitions.setdefault(key(values), []).append(row)
        if not names and not partitions:
            partitions[key(())] = []
        for members in partitions.values():
            values = {name: members[0][name] for name in names} if members else {}
            output = {name: values.get(name) for name in query_dimensions}
            output['instance_id'] = entry['instance_id']
            output['grouping'] = list(entry['grouping'])
            for spec in specs:
                output[spec['name']] = _reference_value(members, spec, entry, query_dimensions)
            result.append(output)
    return result


def _reference_value(members, spec, entry, query_dimensions):
    op = spec['op']
    if op == 'count_all': return len(members)
    if op in {'grouping', 'grouping_id'}:
        omitted = set(query_dimensions) - set(entry['dimensions'])
        bits = [1 if name in omitted else 0 for name in spec['fields']]
        return bits[0] if op == 'grouping' and len(bits) == 1 else int(''.join(map(str, bits)), 2)
    collected = []
    for row in members:
        values = tuple(row[name] for name in spec['fields'])
        if any(value is None for value in values): continue
        collected.append(values)
    if spec['distinct']:
        unique = {}
        for values in collected:
            encoded = key(values)
            if encoded not in unique: unique[encoded] = values
        collected = list(unique.values())
    if op == 'count': return len(collected)
    if not collected: return None
    if op == 'sum': return sum(values[0] for values in collected)
    if op == 'avg': return str(Fraction(sum(values[0] for values in collected), len(collected)))
    if op == 'min': return min(values[0] for values in collected)
    if op == 'max': return max(values[0] for values in collected)
    raise ValueError('聚合不支持')


def _canonical(row):
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def verify(rows, grouping_sets, aggregates, dimensions=None,
           budget=DEFAULT_BUDGET, spill_dir=None):
    """逐实例把执行层结果与独立直接计算核对。"""
    rows = list(rows)
    actual = execute(rows, grouping_sets, aggregates, dimensions, budget, spill_dir)
    expected = reference(rows, grouping_sets, aggregates, dimensions)
    actual_by_instance = {}
    expected_by_instance = {}
    for row in actual['rows']:
        actual_by_instance.setdefault(row['instance_id'], []).append(_canonical(row))
    for row in expected:
        expected_by_instance.setdefault(row['instance_id'], []).append(_canonical(row))
    layers = []
    for entry in actual['plan']:
        instance_id = entry['instance_id']
        left = sorted(actual_by_instance.get(instance_id, []))
        right = sorted(expected_by_instance.get(instance_id, []))
        layers.append({
            'instance_id': instance_id,
            'dimensions': entry['dimensions'],
            'grouping': entry['grouping'],
            'actual_rows': len(left),
            'expected_rows': len(right),
            'ok': left == right,
        })
    return {
        'ok': all(layer['ok'] for layer in layers),
        'dimensions': actual['dimensions'],
        'grouping': actual['grouping'],
        'layers': layers,
        'plan': actual['plan'],
        'rows': actual['rows'],
        'source_rows': actual['source_rows'],
        'metrics': actual['metrics'],
    }


def group(rows, dimensions, aggregates, grouping_sets=None, **kwargs):
    """兼容入口。

    原有三参数调用保持原慢路径。传入 ``grouping_sets``，或把维度位置直接
    写成“列表的列表”时，转入分组集执行层并返回结果行。
    """
    if grouping_sets is None and _looks_like_sets(dimensions) and all(_is_spec(a) for a in aggregates):
        grouping_sets, dimensions = dimensions, None
    if grouping_sets is not None:
        return group_sets(rows, grouping_sets, aggregates, dimensions=dimensions, **kwargs)
    for spec in aggregates:
        if not _is_spec(spec):
            continue
        op = spec.get('op') if isinstance(spec, dict) else spec[1]
        if op in {'grouping', 'grouping_id'}:
            raise ValueError('该入口不支持 GROUPING，请使用 execute')
    return _legacy_group(rows, list(dimensions), aggregates)


def inspect(data, store, root, budget=DEFAULT_BUDGET):
    """读取合成资料并核对多层分组集查询。"""
    rows = data['rows']
    sets = data.get('sets') or data.get('grouping_sets') or [['team']]
    aggregates = [
        ('rows', 'count_all', [], False),
        ('sum', 'sum', ['amount'], False),
        ('users', 'count', ['user'], True),
    ]
    checked = verify(rows, sets, aggregates, budget=budget, spill_dir=root)
    return {
        'grouping': checked['grouping'],
        'dimensions': checked['dimensions'],
        'plan': checked['plan'],
        'rows': checked['rows'],
        'source_rows': checked['source_rows'],
        'metrics': checked['metrics'],
        'verification': {
            'ok': checked['ok'],
            'layers': [{'instance_id': layer['instance_id'], 'dimensions': layer['dimensions'],
                        'grouping': layer['grouping'], 'ok': layer['ok']}
                       for layer in checked['layers']],
        },
    }
