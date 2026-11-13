"""领域资料及现有接口核对。"""
import json,tempfile,unittest
from pathlib import Path
from warehouse.objects import ObjectStore
from warehouse.aggregate import inspect

class DomainTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.store=ObjectStore(self.root/'objects')
        self.data=json.loads(Path('data/source.json').read_text(encoding='utf-8'))
    def test_existing_reader(self):
        result=inspect(self.data,self.store,self.root)
        self.assertTrue(result)

    def test_null_and_empty_group(self):
        from warehouse.aggregate import group
        self.assertEqual(group([],[],[('c','count_all',[],False),('s','sum',['amount'],False)]),[{'c':0,'s':None}])
        self.assertEqual(group([],['team'],[('c','count_all',[],False)]),[])
        r=group(self.data['rows'],[],[('u','count',['user'],True),('s','sum',['amount'],True)])[0]
        self.assertEqual(r,{'u':2,'s':5})
