import unittest
from datetime import datetime

from dataset import connect
from dataset.util import DatasetException
from sample_data import TEST_DATA

class DatabaseTestCase(unittest.TestCase):

    def setUp(self):
        self.db = connect('sqlite:///:memory:')
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)

    def test_tables(self):
        assert self.db.tables==['weather'], self.db.tables

    def test_create_table(self):
        table = self.db['foo']
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert 'id' in table.table.c, table.table.c

    def test_load_table(self):
        tbl = self.db.load_table('weather')
        assert tbl.table==self.tbl.table

    def test_query(self):
        r = self.db.query('SELECT COUNT(*) AS num FROM weather').next()
        assert r['num']==len(TEST_DATA), r


class TableTestCase(unittest.TestCase):

    def setUp(self):
        self.db = connect('sqlite:///:memory:')
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)

    def test_insert(self):
        assert len(self.tbl)==len(TEST_DATA), len(self.tbl)
        self.tbl.insert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'}
            )
        assert len(self.tbl)==len(TEST_DATA)+1, len(self.tbl)

    def test_upsert(self):
        self.tbl.upsert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'},
            ['place']
            )
        assert len(self.tbl)==len(TEST_DATA)+1, len(self.tbl)
        self.tbl.upsert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'},
            ['place']
            )
        assert len(self.tbl)==len(TEST_DATA)+1, len(self.tbl)

    def test_delete(self):
        self.tbl.insert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'}
            )
        assert len(self.tbl)==len(TEST_DATA)+1, len(self.tbl)
        self.tbl.delete(place='Berlin')
        assert len(self.tbl)==len(TEST_DATA), len(self.tbl)

    def test_find_one(self):
        self.tbl.insert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'}
            )
        d = self.tbl.find_one(place='Berlin')
        assert d['temperature']==-10, d
        d = self.tbl.find_one(place='Atlantis')
        assert d is None, d

    def test_find(self):
        ds = list(self.tbl.find(place='Berkeley'))
        assert len(ds)==3, ds
        ds = list(self.tbl.find(place='Berkeley', _limit=2))
        assert len(ds)==2, ds

    def test_distinct(self):
        x = list(self.tbl.distinct('place'))
        assert len(x)==2, x
        x = list(self.tbl.distinct('place', 'date'))
        assert len(x)==6, x

    def test_insert_many(self):
        data = TEST_DATA * 5000
        self.tbl.insert_many(data)
        assert len(self.tbl) == len(data) + 6

    def test_drop_warning(self):
        assert self.tbl._is_dropped is False, 'table shouldn\'t be dropped yet'
        self.tbl.drop()
        assert self.tbl._is_dropped is True, 'table should be dropped now'
        try:
            list(self.tbl.all())
        except DatasetException:
            pass
        else:
            assert False, 'we should not reach else block, no exception raised!'


if __name__ == '__main__':
    unittest.main()

