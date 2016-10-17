import os
import unittest
from datetime import datetime

try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict  # Python < 2.7 drop-in

from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from dataset import connect
from dataset.util import DatasetException

from .sample_data import TEST_DATA, TEST_CITY_1


class DatabaseTestCase(unittest.TestCase):

    def setUp(self):
        os.environ.setdefault('DATABASE_URL', 'sqlite:///:memory:')
        self.db = connect(os.environ['DATABASE_URL'])
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)

    def tearDown(self):
        for table in self.db.tables:
            self.db[table].drop()

    def test_valid_database_url(self):
        assert self.db.url, os.environ['DATABASE_URL']

    def test_database_url_query_string(self):
        db = connect('sqlite:///:memory:/?cached_statements=1')
        assert 'cached_statements' in db.url, db.url

    def test_tables(self):
        assert self.db.tables == ['weather'], self.db.tables

    def test_contains(self):
        assert 'weather' in self.db, self.db.tables

    def test_create_table(self):
        table = self.db['foo']
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert 'id' in table.table.c, table.table.c

    def test_create_table_custom_id1(self):
        pid = "string_id"
        table = self.db.create_table("foo2", pid, 'String')
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({
            'string_id': 'foobar'})
        assert table.find_one(string_id='foobar')['string_id'] == 'foobar'

    def test_create_table_custom_id2(self):
        pid = "string_id"
        table = self.db.create_table("foo3", pid, 'String(50)')
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({
            'string_id': 'foobar'})
        assert table.find_one(string_id='foobar')['string_id'] == 'foobar'

    def test_create_table_custom_id3(self):
        pid = "int_id"
        table = self.db.create_table("foo4", primary_id=pid)
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({'int_id': 123})
        table.insert({'int_id': 124})
        assert table.find_one(int_id=123)['int_id'] == 123
        assert table.find_one(int_id=124)['int_id'] == 124
        self.assertRaises(IntegrityError, lambda: table.insert({'int_id': 123}))

    def test_create_table_shorthand1(self):
        pid = "int_id"
        table = self.db.get_table('foo5', pid)
        assert table.table.exists
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({'int_id': 123})
        table.insert({'int_id': 124})
        assert table.find_one(int_id=123)['int_id'] == 123
        assert table.find_one(int_id=124)['int_id'] == 124
        self.assertRaises(IntegrityError, lambda: table.insert({'int_id': 123}))

    def test_create_table_shorthand2(self):
        pid = "string_id"
        table = self.db.get_table('foo6', primary_id=pid, primary_type='String')
        assert table.table.exists
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({
            'string_id': 'foobar'})
        assert table.find_one(string_id='foobar')['string_id'] == 'foobar'

    def test_create_table_shorthand3(self):
        pid = "string_id"
        table = self.db.get_table('foo7', primary_id=pid, primary_type='String(20)')
        assert table.table.exists
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({
            'string_id': 'foobar'})
        assert table.find_one(string_id='foobar')['string_id'] == 'foobar'

    def test_with(self):
        init_length = len(self.db['weather'])
        with self.assertRaises(ValueError):
            with self.db as tx:
                tx['weather'].insert({'date': datetime(2011, 1, 1), 'temperature': 1, 'place': u'tmp_place'})
                raise ValueError()
        assert len(self.db['weather']) == init_length

    def test_invalid_values(self):
        if 'mysql' in self.db.engine.dialect.dbapi.__name__:
            # WARNING: mysql seems to be doing some weird type casting upon insert.
            # The mysql-python driver is not affected but it isn't compatible with Python 3
            # Conclusion: use postgresql.
            return
        with self.assertRaises(SQLAlchemyError):
            tbl = self.db['weather']
            tbl.insert({'date': True, 'temperature': 'wrong_value', 'place': u'tmp_place'})

    def test_load_table(self):
        tbl = self.db.load_table('weather')
        assert tbl.table == self.tbl.table

    def test_query(self):
        r = self.db.query('SELECT COUNT(*) AS num FROM weather').next()
        assert r['num'] == len(TEST_DATA), r

    def test_table_cache_updates(self):
        tbl1 = self.db.get_table('people')
        data = OrderedDict([('first_name', 'John'), ('last_name', 'Smith')])
        tbl1.insert(data)
        data['id'] = 1
        tbl2 = self.db.get_table('people')
        assert dict(tbl2.all().next()) == dict(data), (tbl2.all().next(), data)


class TableTestCase(unittest.TestCase):

    def setUp(self):
        self.db = connect('sqlite:///:memory:')
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)

    def test_insert(self):
        assert len(self.tbl) == len(TEST_DATA), len(self.tbl)
        last_id = self.tbl.insert({
            'date': datetime(2011, 1, 2),
            'temperature': -10,
            'place': 'Berlin'}
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)
        assert self.tbl.find_one(id=last_id)['place'] == 'Berlin'

    def test_insert_ignore(self):
        self.tbl.insert_ignore({
            'date': datetime(2011, 1, 2),
            'temperature': -10,
            'place': 'Berlin'},
            ['place']
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)
        self.tbl.insert_ignore({
            'date': datetime(2011, 1, 2),
            'temperature': -10,
            'place': 'Berlin'},
            ['place']
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)

    def test_insert_ignore_all_key(self):
        for i in range(0, 2):
            self.tbl.insert_ignore({
                'date': datetime(2011, 1, 2),
                'temperature': -10,
                'place': 'Berlin'},
                ['date', 'temperature', 'place']
            )

    def test_upsert(self):
        self.tbl.upsert({
            'date': datetime(2011, 1, 2),
            'temperature': -10,
            'place': 'Berlin'},
            ['place']
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)
        self.tbl.upsert({
            'date': datetime(2011, 1, 2),
            'temperature': -10,
            'place': 'Berlin'},
            ['place']
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)

    def test_upsert_all_key(self):
        for i in range(0, 2):
            self.tbl.upsert({
                'date': datetime(2011, 1, 2),
                'temperature': -10,
                'place': 'Berlin'},
                ['date', 'temperature', 'place']
            )

    def test_update_while_iter(self):
        for row in self.tbl:
            row['foo'] = 'bar'
            self.tbl.update(row, ['place', 'date'])

    def test_weird_column_names(self):
        with self.assertRaises(ValueError):
            self.tbl.insert({
                'date': datetime(2011, 1, 2),
                'temperature': -10,
                'foo.bar': 'Berlin',
                'qux.bar': 'Huhu'
            })

    def test_invalid_column_names(self):
        tbl = self.db['weather']
        with self.assertRaises(ValueError):
            tbl.insert({None: 'banana'})

        with self.assertRaises(ValueError):
            tbl.insert({'': 'banana'})

        with self.assertRaises(ValueError):
            tbl.insert({'-': 'banana'})

    def test_delete(self):
        self.tbl.insert({
            'date': datetime(2011, 1, 2),
            'temperature': -10,
            'place': 'Berlin'}
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)
        assert self.tbl.delete(place='Berlin') is True, 'should return 1'
        assert len(self.tbl) == len(TEST_DATA), len(self.tbl)
        assert self.tbl.delete() is True, 'should return non zero'
        assert len(self.tbl) == 0, len(self.tbl)

    def test_repr(self):
        assert repr(self.tbl) == '<Table(weather)>', 'the representation should be <Table(weather)>'

    def test_delete_nonexist_entry(self):
        assert self.tbl.delete(place='Berlin') is False, 'entry not exist, should fail to delete'

    def test_find_one(self):
        self.tbl.insert({
            'date': datetime(2011, 1, 2),
            'temperature': -10,
            'place': 'Berlin'}
        )
        d = self.tbl.find_one(place='Berlin')
        assert d['temperature'] == -10, d
        d = self.tbl.find_one(place='Atlantis')
        assert d is None, d

    def test_count(self):
        assert len(self.tbl) == 6, len(self.tbl)
        l = self.tbl.count(place=TEST_CITY_1)
        assert l == 3, l

    def test_find(self):
        ds = list(self.tbl.find(place=TEST_CITY_1))
        assert len(ds) == 3, ds
        ds = list(self.tbl.find(place=TEST_CITY_1, _limit=2))
        assert len(ds) == 2, ds
        ds = list(self.tbl.find(place=TEST_CITY_1, _limit=2, _step=1))
        assert len(ds) == 2, ds
        ds = list(self.tbl.find(place=TEST_CITY_1, _limit=1, _step=2))
        assert len(ds) == 1, ds
        ds = list(self.tbl.find(_step=2))
        assert len(ds) == len(TEST_DATA), ds
        ds = list(self.tbl.find(order_by=['temperature']))
        assert ds[0]['temperature'] == -1, ds
        ds = list(self.tbl.find(order_by=['-temperature']))
        assert ds[0]['temperature'] == 8, ds
        ds = list(self.tbl.find(self.tbl.table.columns.temperature > 4))
        assert len(ds) == 3, ds

    def test_offset(self):
        ds = list(self.tbl.find(place=TEST_CITY_1, _offset=1))
        assert len(ds) == 2, ds
        ds = list(self.tbl.find(place=TEST_CITY_1, _limit=2, _offset=2))
        assert len(ds) == 1, ds

    def test_distinct(self):
        x = list(self.tbl.distinct('place'))
        assert len(x) == 2, x
        x = list(self.tbl.distinct('place', 'date'))
        assert len(x) == 6, x
        x = list(self.tbl.distinct(
            'place', 'date',
            self.tbl.table.columns.date >= datetime(2011, 1, 2, 0, 0)))
        assert len(x) == 4, x

    def test_get_items(self):
        x = list(self.tbl['place'])
        y = list(self.tbl.distinct('place'))
        assert x == y, (x, y)
        x = list(self.tbl['place', 'date'])
        y = list(self.tbl.distinct('place', 'date'))
        assert x == y, (x, y)

    def test_insert_many(self):
        data = TEST_DATA * 100
        self.tbl.insert_many(data, chunk_size=13)
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

    def test_table_drop(self):
        assert 'weather' in self.db
        self.db['weather'].drop()
        assert 'weather' not in self.db

    def test_columns(self):
        cols = self.tbl.columns
        assert len(list(cols)) == 4, 'column count mismatch'
        assert 'date' in cols and 'temperature' in cols and 'place' in cols

    def test_drop_column(self):
        try:
            self.tbl.drop_column('date')
            assert 'date' not in self.tbl.columns
        except NotImplementedError:
            pass

    def test_iter(self):
        c = 0
        for row in self.tbl:
            c += 1
        assert c == len(self.tbl)

    def test_update(self):
        date = datetime(2011, 1, 2)
        res = self.tbl.update({
            'date': date,
            'temperature': -10,
            'place': TEST_CITY_1},
            ['place', 'date']
        )
        assert res, 'update should return True'
        m = self.tbl.find_one(place=TEST_CITY_1, date=date)
        assert m['temperature'] == -10, 'new temp. should be -10 but is %d' % m['temperature']

    def test_create_column(self):
        from sqlalchemy import FLOAT
        tbl = self.tbl
        tbl.create_column('foo', FLOAT)
        assert 'foo' in tbl.table.c, tbl.table.c
        assert isinstance(tbl.table.c['foo'].type, FLOAT), tbl.table.c['foo'].type
        assert 'foo' in tbl.columns, tbl.columns

    def test_key_order(self):
        res = self.db.query('SELECT temperature, place FROM weather LIMIT 1')
        keys = list(res.next().keys())
        assert keys[0] == 'temperature'
        assert keys[1] == 'place'

    def test_empty_query(self):
        m = self.tbl.find(place='not in data')
        l = list(m)  # exhaust iterator
        assert len(l) == 0


class Constructor(dict):
    """ Very simple low-functionality extension to ``dict`` to
    provide attribute access to dictionary contents"""
    def __getattr__(self, name):
        return self[name]


class RowTypeTestCase(unittest.TestCase):

    def setUp(self):
        self.db = connect('sqlite:///:memory:', row_type=Constructor)
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)

    def tearDown(self):
        for table in self.db.tables:
            self.db[table].drop()

    def test_find_one(self):
        self.tbl.insert({
            'date': datetime(2011, 1, 2),
            'temperature': -10,
            'place': 'Berlin'}
        )
        d = self.tbl.find_one(place='Berlin')
        assert d['temperature'] == -10, d
        assert d.temperature == -10, d
        d = self.tbl.find_one(place='Atlantis')
        assert d is None, d

    def test_find(self):
        ds = list(self.tbl.find(place=TEST_CITY_1))
        assert len(ds) == 3, ds
        for item in ds:
            assert isinstance(item, Constructor), item
        ds = list(self.tbl.find(place=TEST_CITY_1, _limit=2))
        assert len(ds) == 2, ds
        for item in ds:
            assert isinstance(item, Constructor), item

    def test_distinct(self):
        x = list(self.tbl.distinct('place'))
        assert len(x) == 2, x
        for item in x:
            assert isinstance(item, Constructor), item
        x = list(self.tbl.distinct('place', 'date'))
        assert len(x) == 6, x
        for item in x:
            assert isinstance(item, Constructor), item

    def test_iter(self):
        c = 0
        for row in self.tbl:
            c += 1
            assert isinstance(row, Constructor), row
        assert c == len(self.tbl)


if __name__ == '__main__':
    unittest.main()
