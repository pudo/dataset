import os
import unittest
from datetime import datetime

from dataset import connect
from dataset.util import DatasetException
from sample_data import TEST_DATA
from sqlalchemy.exc import IntegrityError


class DatabaseTestCase(unittest.TestCase):

    def setUp(self):
        os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
        self.db = connect('sqlite:///:memory:')
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)

    def tearDown(self):
        # ensure env variable was unset
        del os.environ['DATABASE_URL']

    def test_valid_database_url(self):
        assert self.db.url, os.environ['DATABASE_URL']

    def test_database_url_query_string(self):
        db = connect('sqlite:///:memory:/?cached_statements=1')
        assert 'cached_statements' in db.url, db.url

    def test_tables(self):
        assert self.db.tables == ['weather'], self.db.tables

    def test_create_table(self):
        table = self.db['foo']
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert 'id' in table.table.c, table.table.c

    def test_create_table_custom_id1(self):
        pid = "string_id"
        table = self.db.create_table("foo2", pid, 'Text')
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({
            'string_id': 'foobar'})
        assert table.find_one(string_id = 'foobar')['string_id'] == 'foobar'

    def test_create_table_custom_id2(self):
        pid = "int_id"
        table = self.db.create_table("foo3", primary_id = pid)
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({'int_id': 123})
        table.insert({'int_id': 124})
        assert table.find_one(int_id = 123)['int_id'] == 123
        assert table.find_one(int_id = 124)['int_id'] == 124
        with self.assertRaises(IntegrityError):
            table.insert({'int_id': 123})

    def test_create_table_shorthand1(self):
        pid = "int_id"
        table = self.db['foo4', pid]
        assert table.table.exists
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({'int_id': 123})
        table.insert({'int_id': 124})
        assert table.find_one(int_id = 123)['int_id'] == 123
        assert table.find_one(int_id = 124)['int_id'] == 124
        with self.assertRaises(IntegrityError):
            table.insert({'int_id': 123})

    def test_create_table_shorthand2(self):
        pid = "string_id"
        table = self.db['foo5', pid, 'Text']
        assert table.table.exists
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({
            'string_id': 'foobar'})
        assert table.find_one(string_id = 'foobar')['string_id'] == 'foobar'

    def test_load_table(self):
        tbl = self.db.load_table('weather')
        assert tbl.table == self.tbl.table

    def test_query(self):
        r = self.db.query('SELECT COUNT(*) AS num FROM weather').next()
        assert r['num'] == len(TEST_DATA), r


class TableTestCase(unittest.TestCase):

    def setUp(self):
        self.db = connect('sqlite:///:memory:')
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)

    def test_insert(self):
        assert len(self.tbl) == len(TEST_DATA), len(self.tbl)
        last_id = self.tbl.insert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'}
        )
        assert len(self.tbl) == len(TEST_DATA)+1, len(self.tbl)
        assert self.tbl.find_one(id=last_id)['place'] == 'Berlin'

    def test_upsert(self):
        self.tbl.upsert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'},
            ['place']
        )
        assert len(self.tbl) == len(TEST_DATA)+1, len(self.tbl)
        self.tbl.upsert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'},
            ['place']
        )
        assert len(self.tbl) == len(TEST_DATA)+1, len(self.tbl)

    def test_upsert_all_key(self):
        for i in range(0,2):
            self.tbl.upsert({
                'date': datetime(2011, 01, 02),
                'temperature': -10,
                'place': 'Berlin'},
                ['date', 'temperature', 'place']
            )

    def test_delete(self):
        self.tbl.insert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'}
        )
        assert len(self.tbl) == len(TEST_DATA)+1, len(self.tbl)
        self.tbl.delete(place='Berlin')
        assert len(self.tbl) == len(TEST_DATA), len(self.tbl)
        self.tbl.delete()
        assert len(self.tbl) == 0, len(self.tbl)

    def test_find_one(self):
        self.tbl.insert({
            'date': datetime(2011, 01, 02),
            'temperature': -10,
            'place': 'Berlin'}
        )
        d = self.tbl.find_one(place='Berlin')
        assert d['temperature'] == -10, d
        d = self.tbl.find_one(place='Atlantis')
        assert d is None, d

    def test_find(self):
        ds = list(self.tbl.find(place='Berkeley'))
        assert len(ds) == 3, ds
        ds = list(self.tbl.find(place='Berkeley', _limit=2))
        assert len(ds) == 2, ds

    def test_distinct(self):
        x = list(self.tbl.distinct('place'))
        assert len(x) == 2, x
        x = list(self.tbl.distinct('place', 'date'))
        assert len(x) == 6, x

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

    def test_columns(self):
        cols = self.tbl.columns
        assert isinstance(cols, set), 'columns should be a set'
        assert len(cols) == 4, 'column count mismatch'
        assert 'date' in cols and 'temperature' in cols and 'place' in cols

    def test_iter(self):
        c = 0
        for row in self.tbl:
            c += 1
        assert c == len(self.tbl)

    def test_update(self):
        date = datetime(2011, 01, 02)
        res = self.tbl.update({
            'date': date,
            'temperature': -10,
            'place': 'Berkeley'},
            ['place', 'date']
        )
        assert res, 'update should return True'
        m = self.tbl.find_one(place='Berkeley', date=date)
        assert m['temperature'] == -10, 'new temp. should be -10 but is %d' % m['temperature']

    def test_create_column(self):
        from sqlalchemy import FLOAT
        tbl = self.tbl
        tbl.create_column('foo', FLOAT)
        assert 'foo' in tbl.table.c, tbl.table.c
        assert FLOAT == type(tbl.table.c['foo'].type), tbl.table.c['foo'].type
        assert 'foo' in tbl.columns, tbl.columns

if __name__ == '__main__':
    unittest.main()
