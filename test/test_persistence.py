import unittest
from datetime import datetime

from dataset import connect
from sample_data import TEST_DATA

class DatabaseTestCase(unittest.TestCase):

    def setUp(self):
        self.db = connect('sqlite:///:memory:')
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)

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

    def test_distinct(self):
        x = list(self.tbl.distinct('place'))
        assert len(x)==2, x
        x = list(self.tbl.distinct('place', 'date'))
        assert len(x)==6, x



if __name__ == '__main__':
    unittest.main()

