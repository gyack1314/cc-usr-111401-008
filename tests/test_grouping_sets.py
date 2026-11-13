"""分组集、GROUPING 标记与去重状态边界的核对。"""
import json
import tempfile
import unittest
from pathlib import Path

from warehouse.aggregate import (
    aggregate, execute, expand, group, group_sets, grouping, grouping_id,
    grouping_sets, inspect, plan, reference, verify,
)

ROWS = [
    {'row_id': 'a', 'team': None, 'tool': 'chat', 'plan': 'pro', 'amount': 5, 'user': 'u1'},
    {'row_id': 'b', 'team': 'ops', 'tool': 'chat', 'plan': 'pro', 'amount': 5, 'user': 'u1'},
    {'row_id': 'c', 'team': 'ops', 'tool': 'chat', 'plan': 'basic', 'amount': None, 'user': 'u2'},
]


class GroupingSetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.aggs = [('rows', 'count_all', [], False), ('sum', 'sum', ['amount'], False),
                     ('users', 'count', ['user'], True)]

    def test_expand_plan(self):
        dimensions, entries = expand([['team', 'tool'], ['team'], [], []])
        self.assertEqual(dimensions, ['team', 'tool'])
        self.assertEqual([e['instance_id'] for e in entries], [0, 1, 2, 3])
        self.assertEqual([e['grouping'] for e in entries], [[0, 0], [0, 1], [1, 1], [1, 1]])
        self.assertEqual(plan([['a'], []])['plan'][1]['grouping'], [1])
        self.assertEqual(grouping_id(['a', 'b'], ['b']), 2)
        self.assertEqual(grouping(['a', 'b'], []), [1, 1])

    def test_layers_and_source_null(self):
        result = execute(ROWS, [['team'], []], self.aggs)
        by_grouping = {}
        for row in result['rows']:
            by_grouping.setdefault(tuple(row['grouping']), []).append(row)
        self.assertEqual(sorted(r['team'] for r in by_grouping[(0,)] if r['team'] is not None),
                         ['ops'])
        real_null = [r for r in by_grouping[(0,)] if r['team'] is None]
        self.assertEqual(len(real_null), 1)
        self.assertEqual(real_null[0]['rows'], 1)
        total = by_grouping[(1,)][0]
        self.assertIsNone(total['team'])
        self.assertEqual((total['rows'], total['sum'], total['users']), (3, 10, 2))

    def test_duplicate_sets_have_instances(self):
        result = execute(ROWS, [['team'], ['team']], self.aggs)
        instances = {}
        for row in result['rows']:
            instances.setdefault(row['instance_id'], []).append(row)
        self.assertEqual(sorted(instances), [0, 1])
        def strip(rows):
            return sorted(json.dumps({k: v for k, v in row.items() if k != 'instance_id'},
                                     sort_keys=True) for row in rows)
        self.assertEqual(strip(instances[0]), strip(instances[1]))

    def test_distinct_parameters_do_not_mix(self):
        result = execute(ROWS, [[], ['team']], [('u', 'count', ['user'], True),
                                                ('ut', 'count', ['user', 'tool'], True),
                                                ('t', 'count', ['tool'], True)])
        by_instance = {row['instance_id']: row for row in result['rows']
                       if row['instance_id'] == 0}
        self.assertEqual(by_instance[0]['grouping'], [1])
        self.assertEqual((by_instance[0]['u'], by_instance[0]['ut'], by_instance[0]['t']),
                         (2, 2, 1))

    def test_grouping_marker(self):
        result = execute(ROWS, [['team'], []], [('g', 'grouping', ['team'], False),
                                                ('gid', 'grouping_id', ['team'], False)])
        by_grouping = {tuple(r['grouping']): r for r in result['rows']}
        self.assertEqual(by_grouping[(0,)]['g'], 0)
        self.assertEqual(by_grouping[(0,)]['gid'], 0)
        self.assertEqual(by_grouping[(1,)]['g'], 1)
        self.assertEqual(by_grouping[(1,)]['gid'], 1)

    def test_empty_input(self):
        empty = execute([], [[]], self.aggs, dimensions=['team'])
        self.assertEqual(empty['rows'], [{'instance_id': 0, 'grouping': [1],
                                          'rows': 0, 'sum': None, 'users': 0, 'team': None}])
        self.assertEqual(execute([], [['team']], self.aggs)['rows'], [])

    def test_bag_semantics(self):
        duplicate = [{'row_id': 'a', 'team': 'x', 'amount': 1, 'user': 'u'},
                     {'row_id': 'b', 'team': 'x', 'amount': 1, 'user': 'u'}]
        result = execute(duplicate, [['team']], self.aggs)
        self.assertEqual(result['rows'][0]['rows'], 2)
        self.assertEqual(result['rows'][0]['sum'], 2)
        self.assertEqual(result['rows'][0]['users'], 1)

    def test_verify_matches_reference(self):
        checked = verify(ROWS, [['team', 'tool'], ['team'], [], []], self.aggs)
        self.assertTrue(checked['ok'])
        self.assertEqual([layer['instance_id'] for layer in checked['layers']], [0, 1, 2, 3])
        self.assertEqual(checked['metrics']['scans'], 1)
        self.assertEqual(checked['metrics']['source_rows'], 3)

    def test_budget_and_spill_cleanup(self):
        rows = [{'row_id': str(i), 'user': 'u%d' % i, 'amount': i} for i in range(2000)]
        with tempfile.TemporaryDirectory() as directory:
            checked = verify(rows, [['user'], []], [('u', 'count', ['user'], True),
                                                   ('s', 'sum', ['amount'], True)],
                             budget=7, spill_dir=directory)
            self.assertTrue(checked['ok'])
            self.assertLessEqual(checked['metrics']['peak_distinct_entries'], 7)
            self.assertGreater(checked['metrics']['spills'], 0)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_bad_sets(self):
        with self.assertRaises(ValueError):
            execute(ROWS, [], self.aggs)
        with self.assertRaises(ValueError):
            execute(ROWS, [['team', 'team']], self.aggs)
        with self.assertRaises(ValueError):
            execute(ROWS, [['team', 'tool']], self.aggs, dimensions=['team'])
        with self.assertRaises(ValueError):
            execute(ROWS, [['team']], [('rows', 'count_all', [], False),
                                       ('rows', 'count_all', [], False)])
        with self.assertRaises(ValueError):
            execute(ROWS, [['team']], [('grouping', 'min', ['amount'], False)])
        with self.assertRaises(ValueError):
            execute([{'team': 'a'}], [['missing']], [])

    def test_compatibility_aliases(self):
        self.assertEqual(group_sets(ROWS, [['team']], self.aggs),
                         execute(ROWS, [['team']], self.aggs)['rows'])
        self.assertEqual(grouping_sets(ROWS, [[]], self.aggs),
                         aggregate(ROWS, [[]], self.aggs))
        self.assertTrue(verify(ROWS, [['team'], []], self.aggs)['ok'])
        grouped = execute(ROWS, [['team', 'tool'], []], self.aggs, dimensions=['team', 'tool'])
        self.assertTrue(grouped['metrics']['scans'] == 1)
        self.assertEqual(grouped['dimensions'], ['team', 'tool'])
        self.assertEqual(plan([['team', 'tool'], []])['plan'][1]['grouping'], [1, 1])
        with self.assertRaises(ValueError):
            group(ROWS, ['team'], [('g', 'grouping', ['team'], False)])

    def test_legacy_group_shape(self):
        legacy = group(ROWS, ['team'], [('c', 'count_all', [], False)])
        self.assertEqual(legacy, [{'team': None, 'c': 1}, {'team': 'ops', 'c': 2}])
        self.assertEqual(group([], [], [('c', 'count_all', [], False)]), [{'c': 0}])

    def test_existing_inspect_still_works(self):
        data = json.loads(Path('data/source.json').read_text(encoding='utf-8'))
        result = inspect(data, None, self.root)
        self.assertTrue(result['verification']['ok'])
        self.assertEqual(result['source_rows'], 3)
        self.assertEqual(len(result['plan']), 4)


if __name__ == '__main__':
    unittest.main()
