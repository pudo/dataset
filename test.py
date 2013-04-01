import unittest
from datetime import datetime

from sqlaload import *

TEST_DATA = [
    {
        'date': datetime(2011, 01, 01),
        'temperature': 1,
        'place': 'Galway'
    },
    {
        'date': datetime(2011, 01, 02),
        'temperature': -1,
        'place': 'Galway'
    },
    {
        'date': datetime(2011, 01, 03),
        'temperature': 0,
        'place': 'Galway'
    },
    {
        'date': datetime(2011, 01, 01),
        'temperature': 6,
        'place': 'Berkeley'
    },
    {
        'date': datetime(2011, 01, 02),
        'temperature': 8,
        'place': 'Berkeley'
    },
    {
        'date': datetime(2011, 01, 03),
        'temperature': 5,
        'place': 'Berkeley'
    }
]


class TableTestCase(unittest.TestCase):

    def setUp(self):
        self.engine = connect('sqlite:///:memory:')

    def test_create_table(self):
        table = create_table(self.engine, 'foo')
        assert table.exists()
        assert len(table.columns) == 1, table.columns


class QueryTestCase(unittest.TestCase):

    def setUp(self):
        self.engine = connect('sqlite:///:memory:')
        self.table = create_table(self.engine, 'weather')
        for entry in TEST_DATA:
            upsert(self.engine, self.table, entry, ['place', 'date'],
                   ensure=True)

    def test_all(self):
        x = list(all(self.engine, self.table))
        assert len(x) == len(TEST_DATA), len(x)

    def test_distinct(self):
        x = list(distinct(self.engine, self.table, 'place'))
        assert len(x) == 2, x
        p = [i['place'] for i in x]
        assert 'Berkeley' in p, p
        assert 'Galway' in p, p


class ObjectAPITestCase(unittest.TestCase):

    def setUp(self):
        db = create('sqlite:///:memory:')
        self.table = db.get_table('weather2')
        for entry in TEST_DATA:
            self.table.upsert(entry, ['place', 'date'], ensure=True)

    def test_all(self):
        x = list(self.table.all())
        assert len(x) == len(TEST_DATA), len(x)

    def test_distinct(self):
        x = list(self.table.distinct('place'))
        assert len(x) == 2, x
        p = [i['place'] for i in x]
        assert 'Berkeley' in p, p
        assert 'Galway' in p, p


if __name__ == '__main__':
    unittest.main()
