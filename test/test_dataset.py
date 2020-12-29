import os
import unittest
from datetime import datetime
from collections import OrderedDict
from sqlalchemy import TEXT, BIGINT
from sqlalchemy.exc import IntegrityError, SQLAlchemyError, ArgumentError

from dataset import connect, chunked

from .sample_data import TEST_DATA, TEST_CITY_1


class DatabaseTestCase(unittest.TestCase):
    def setUp(self):
        self.db = connect()
        self.tbl = self.db["weather"]
        self.tbl.insert_many(TEST_DATA)

    def tearDown(self):
        for table in self.db.tables:
            self.db[table].drop()

    def test_valid_database_url(self):
        assert self.db.url, os.environ["DATABASE_URL"]

    def test_database_url_query_string(self):
        db = connect("sqlite:///:memory:/?cached_statements=1")
        assert "cached_statements" in db.url, db.url

    def test_tables(self):
        assert self.db.tables == ["weather"], self.db.tables

    def test_contains(self):
        assert "weather" in self.db, self.db.tables

    def test_create_table(self):
        table = self.db["foo"]
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert "id" in table.table.c, table.table.c

    def test_create_table_no_ids(self):
        if "mysql" in self.db.engine.dialect.dbapi.__name__:
            return
        if "sqlite" in self.db.engine.dialect.dbapi.__name__:
            return
        table = self.db.create_table("foo_no_id", primary_id=False)
        assert table.table.exists()
        assert len(table.table.columns) == 0, table.table.columns

    def test_create_table_custom_id1(self):
        pid = "string_id"
        table = self.db.create_table("foo2", pid, self.db.types.string(255))
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c
        table.insert({pid: "foobar"})
        assert table.find_one(string_id="foobar")[pid] == "foobar"

    def test_create_table_custom_id2(self):
        pid = "string_id"
        table = self.db.create_table("foo3", pid, self.db.types.string(50))
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({pid: "foobar"})
        assert table.find_one(string_id="foobar")[pid] == "foobar"

    def test_create_table_custom_id3(self):
        pid = "int_id"
        table = self.db.create_table("foo4", primary_id=pid)
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({pid: 123})
        table.insert({pid: 124})
        assert table.find_one(int_id=123)[pid] == 123
        assert table.find_one(int_id=124)[pid] == 124
        self.assertRaises(IntegrityError, lambda: table.insert({pid: 123}))

    def test_create_table_shorthand1(self):
        pid = "int_id"
        table = self.db.get_table("foo5", pid)
        assert table.table.exists
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({"int_id": 123})
        table.insert({"int_id": 124})
        assert table.find_one(int_id=123)["int_id"] == 123
        assert table.find_one(int_id=124)["int_id"] == 124
        self.assertRaises(IntegrityError, lambda: table.insert({"int_id": 123}))

    def test_create_table_shorthand2(self):
        pid = "string_id"
        table = self.db.get_table(
            "foo6", primary_id=pid, primary_type=self.db.types.string(255)
        )
        assert table.table.exists
        assert len(table.table.columns) == 1, table.table.columns
        assert pid in table.table.c, table.table.c

        table.insert({"string_id": "foobar"})
        assert table.find_one(string_id="foobar")["string_id"] == "foobar"

    def test_with(self):
        init_length = len(self.db["weather"])
        with self.assertRaises(ValueError):
            with self.db as tx:
                tx["weather"].insert(
                    {
                        "date": datetime(2011, 1, 1),
                        "temperature": 1,
                        "place": "tmp_place",
                    }
                )
                raise ValueError()
        assert len(self.db["weather"]) == init_length

    def test_invalid_values(self):
        if "mysql" in self.db.engine.dialect.dbapi.__name__:
            # WARNING: mysql seems to be doing some weird type casting
            # upon insert. The mysql-python driver is not affected but
            # it isn't compatible with Python 3
            # Conclusion: use postgresql.
            return
        with self.assertRaises(SQLAlchemyError):
            tbl = self.db["weather"]
            tbl.insert(
                {"date": True, "temperature": "wrong_value", "place": "tmp_place"}
            )

    def test_load_table(self):
        tbl = self.db.load_table("weather")
        assert tbl.table.name == self.tbl.table.name

    def test_query(self):
        r = self.db.query("SELECT COUNT(*) AS num FROM weather").next()
        assert r["num"] == len(TEST_DATA), r

    def test_table_cache_updates(self):
        tbl1 = self.db.get_table("people")
        data = OrderedDict([("first_name", "John"), ("last_name", "Smith")])
        tbl1.insert(data)
        data["id"] = 1
        tbl2 = self.db.get_table("people")
        assert dict(tbl2.all().next()) == dict(data), (tbl2.all().next(), data)


class TableTestCase(unittest.TestCase):
    def setUp(self):
        self.db = connect()
        self.tbl = self.db["weather"]
        for row in TEST_DATA:
            self.tbl.insert(row)

    def tearDown(self):
        self.tbl.drop()

    def test_insert(self):
        assert len(self.tbl) == len(TEST_DATA), len(self.tbl)
        last_id = self.tbl.insert(
            {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"}
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)
        assert self.tbl.find_one(id=last_id)["place"] == "Berlin"

    def test_insert_ignore(self):
        self.tbl.insert_ignore(
            {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"},
            ["place"],
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)
        self.tbl.insert_ignore(
            {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"},
            ["place"],
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)

    def test_insert_ignore_all_key(self):
        for i in range(0, 4):
            self.tbl.insert_ignore(
                {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"},
                ["date", "temperature", "place"],
            )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)

    def test_insert_json(self):
        last_id = self.tbl.insert(
            {
                "date": datetime(2011, 1, 2),
                "temperature": -10,
                "place": "Berlin",
                "info": {
                    "currency": "EUR",
                    "language": "German",
                    "population": 3292365,
                },
            }
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)
        assert self.tbl.find_one(id=last_id)["place"] == "Berlin"

    def test_upsert(self):
        self.tbl.upsert(
            {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"},
            ["place"],
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)
        self.tbl.upsert(
            {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"},
            ["place"],
        )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)

    def test_upsert_single_column(self):
        table = self.db["banana_single_col"]
        table.upsert({"color": "Yellow"}, ["color"])
        assert len(table) == 1, len(table)
        table.upsert({"color": "Yellow"}, ["color"])
        assert len(table) == 1, len(table)

    def test_upsert_all_key(self):
        assert len(self.tbl) == len(TEST_DATA), len(self.tbl)
        for i in range(0, 2):
            self.tbl.upsert(
                {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"},
                ["date", "temperature", "place"],
            )
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)

    def test_upsert_id(self):
        table = self.db["banana_with_id"]
        data = dict(id=10, title="I am a banana!")
        table.upsert(data, ["id"])
        assert len(table) == 1, len(table)

    def test_update_while_iter(self):
        for row in self.tbl:
            row["foo"] = "bar"
            self.tbl.update(row, ["place", "date"])
        assert len(self.tbl) == len(TEST_DATA), len(self.tbl)

    def test_weird_column_names(self):
        with self.assertRaises(ValueError):
            self.tbl.insert(
                {
                    "date": datetime(2011, 1, 2),
                    "temperature": -10,
                    "foo.bar": "Berlin",
                    "qux.bar": "Huhu",
                }
            )

    def test_cased_column_names(self):
        tbl = self.db["cased_column_names"]
        tbl.insert({"place": "Berlin"})
        tbl.insert({"Place": "Berlin"})
        tbl.insert({"PLACE ": "Berlin"})
        assert len(tbl.columns) == 2, tbl.columns
        assert len(list(tbl.find(Place="Berlin"))) == 3
        assert len(list(tbl.find(place="Berlin"))) == 3
        assert len(list(tbl.find(PLACE="Berlin"))) == 3

    def test_invalid_column_names(self):
        tbl = self.db["weather"]
        with self.assertRaises(ValueError):
            tbl.insert({None: "banana"})

        with self.assertRaises(ValueError):
            tbl.insert({"": "banana"})

        with self.assertRaises(ValueError):
            tbl.insert({"-": "banana"})

    def test_delete(self):
        self.tbl.insert(
            {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"}
        )
        original_count = len(self.tbl)
        assert len(self.tbl) == len(TEST_DATA) + 1, len(self.tbl)
        # Test bad use of API
        with self.assertRaises(ArgumentError):
            self.tbl.delete({"place": "Berlin"})
        assert len(self.tbl) == original_count, len(self.tbl)

        assert self.tbl.delete(place="Berlin") is True, "should return 1"
        assert len(self.tbl) == len(TEST_DATA), len(self.tbl)
        assert self.tbl.delete() is True, "should return non zero"
        assert len(self.tbl) == 0, len(self.tbl)

    def test_repr(self):
        assert (
            repr(self.tbl) == "<Table(weather)>"
        ), "the representation should be <Table(weather)>"

    def test_delete_nonexist_entry(self):
        assert (
            self.tbl.delete(place="Berlin") is False
        ), "entry not exist, should fail to delete"

    def test_find_one(self):
        self.tbl.insert(
            {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"}
        )
        d = self.tbl.find_one(place="Berlin")
        assert d["temperature"] == -10, d
        d = self.tbl.find_one(place="Atlantis")
        assert d is None, d

    def test_count(self):
        assert len(self.tbl) == 6, len(self.tbl)
        length = self.tbl.count(place=TEST_CITY_1)
        assert length == 3, length

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
        ds = list(self.tbl.find(order_by=["temperature"]))
        assert ds[0]["temperature"] == -1, ds
        ds = list(self.tbl.find(order_by=["-temperature"]))
        assert ds[0]["temperature"] == 8, ds
        ds = list(self.tbl.find(self.tbl.table.columns.temperature > 4))
        assert len(ds) == 3, ds

    def test_find_dsl(self):
        ds = list(self.tbl.find(place={"like": "%lw%"}))
        assert len(ds) == 3, ds
        ds = list(self.tbl.find(temperature={">": 5}))
        assert len(ds) == 2, ds
        ds = list(self.tbl.find(temperature={">=": 5}))
        assert len(ds) == 3, ds
        ds = list(self.tbl.find(temperature={"<": 0}))
        assert len(ds) == 1, ds
        ds = list(self.tbl.find(temperature={"<=": 0}))
        assert len(ds) == 2, ds
        ds = list(self.tbl.find(temperature={"!=": -1}))
        assert len(ds) == 5, ds
        ds = list(self.tbl.find(temperature={"between": [5, 8]}))
        assert len(ds) == 3, ds
        ds = list(self.tbl.find(place={"=": "G€lway"}))
        assert len(ds) == 3, ds
        ds = list(self.tbl.find(place={"ilike": "%LwAy"}))
        assert len(ds) == 3, ds

    def test_offset(self):
        ds = list(self.tbl.find(place=TEST_CITY_1, _offset=1))
        assert len(ds) == 2, ds
        ds = list(self.tbl.find(place=TEST_CITY_1, _limit=2, _offset=2))
        assert len(ds) == 1, ds

    def test_streamed(self):
        ds = list(self.tbl.find(place=TEST_CITY_1, _streamed=True, _step=1))
        assert len(ds) == 3, len(ds)
        for row in self.tbl.find(place=TEST_CITY_1, _streamed=True, _step=1):
            row["temperature"] = -1
            self.tbl.update(row, ["id"])

    def test_distinct(self):
        x = list(self.tbl.distinct("place"))
        assert len(x) == 2, x
        x = list(self.tbl.distinct("place", "date"))
        assert len(x) == 6, x
        x = list(
            self.tbl.distinct(
                "place",
                "date",
                self.tbl.table.columns.date >= datetime(2011, 1, 2, 0, 0),
            )
        )
        assert len(x) == 4, x

        x = list(self.tbl.distinct("temperature", place="B€rkeley"))
        assert len(x) == 3, x
        x = list(self.tbl.distinct("temperature", place=["B€rkeley", "G€lway"]))
        assert len(x) == 6, x

    def test_insert_many(self):
        data = TEST_DATA * 100
        self.tbl.insert_many(data, chunk_size=13)
        assert len(self.tbl) == len(data) + 6, (len(self.tbl), len(data))

    def test_chunked_insert(self):
        data = TEST_DATA * 100
        with chunked.ChunkedInsert(self.tbl) as chunk_tbl:
            for item in data:
                chunk_tbl.insert(item)
        assert len(self.tbl) == len(data) + 6, (len(self.tbl), len(data))

    def test_chunked_insert_callback(self):
        data = TEST_DATA * 100
        N = 0

        def callback(queue):
            nonlocal N
            N += len(queue)

        with chunked.ChunkedInsert(self.tbl, callback=callback) as chunk_tbl:
            for item in data:
                chunk_tbl.insert(item)
        assert len(data) == N
        assert len(self.tbl) == len(data) + 6

    def test_update_many(self):
        tbl = self.db["update_many_test"]
        tbl.insert_many([dict(temp=10), dict(temp=20), dict(temp=30)])
        tbl.update_many([dict(id=1, temp=50), dict(id=3, temp=50)], "id")

        # Ensure data has been updated.
        assert tbl.find_one(id=1)["temp"] == tbl.find_one(id=3)["temp"]

    def test_chunked_update(self):
        tbl = self.db["update_many_test"]
        tbl.insert_many(
            [
                dict(temp=10, location="asdf"),
                dict(temp=20, location="qwer"),
                dict(temp=30, location="asdf"),
            ]
        )

        chunked_tbl = chunked.ChunkedUpdate(tbl, "id")
        chunked_tbl.update(dict(id=1, temp=50))
        chunked_tbl.update(dict(id=2, location="asdf"))
        chunked_tbl.update(dict(id=3, temp=50))
        chunked_tbl.flush()

        # Ensure data has been updated.
        assert tbl.find_one(id=1)["temp"] == tbl.find_one(id=3)["temp"] == 50
        assert (
            tbl.find_one(id=2)["location"] == tbl.find_one(id=3)["location"] == "asdf"
        )  # noqa

    def test_upsert_many(self):
        # Also tests updating on records with different attributes
        tbl = self.db["upsert_many_test"]

        W = 100
        tbl.upsert_many([dict(age=10), dict(weight=W)], "id")
        assert tbl.find_one(id=1)["age"] == 10

        tbl.upsert_many([dict(id=1, age=70), dict(id=2, weight=W / 2)], "id")
        assert tbl.find_one(id=2)["weight"] == W / 2

    def test_drop_operations(self):
        assert self.tbl._table is not None, "table shouldn't be dropped yet"
        self.tbl.drop()
        assert self.tbl._table is None, "table should be dropped now"
        assert list(self.tbl.all()) == [], self.tbl.all()
        assert self.tbl.count() == 0, self.tbl.count()

    def test_table_drop(self):
        assert "weather" in self.db
        self.db["weather"].drop()
        assert "weather" not in self.db

    def test_table_drop_then_create(self):
        assert "weather" in self.db
        self.db["weather"].drop()
        assert "weather" not in self.db
        self.db["weather"].insert({"foo": "bar"})

    def test_columns(self):
        cols = self.tbl.columns
        assert len(list(cols)) == 4, "column count mismatch"
        assert "date" in cols and "temperature" in cols and "place" in cols

    def test_drop_column(self):
        try:
            self.tbl.drop_column("date")
            assert "date" not in self.tbl.columns
        except RuntimeError:
            pass

    def test_iter(self):
        c = 0
        for row in self.tbl:
            c += 1
        assert c == len(self.tbl)

    def test_update(self):
        date = datetime(2011, 1, 2)
        res = self.tbl.update(
            {"date": date, "temperature": -10, "place": TEST_CITY_1}, ["place", "date"]
        )
        assert res, "update should return True"
        m = self.tbl.find_one(place=TEST_CITY_1, date=date)
        assert m["temperature"] == -10, (
            "new temp. should be -10 but is %d" % m["temperature"]
        )

    def test_create_column(self):
        tbl = self.tbl
        flt = self.db.types.float
        tbl.create_column("foo", flt)
        assert "foo" in tbl.table.c, tbl.table.c
        assert isinstance(tbl.table.c["foo"].type, flt), tbl.table.c["foo"].type
        assert "foo" in tbl.columns, tbl.columns

    def test_ensure_column(self):
        tbl = self.tbl
        flt = self.db.types.float
        tbl.create_column_by_example("foo", 0.1)
        assert "foo" in tbl.table.c, tbl.table.c
        assert isinstance(tbl.table.c["foo"].type, flt), tbl.table.c["bar"].type
        tbl.create_column_by_example("bar", 1)
        assert "bar" in tbl.table.c, tbl.table.c
        assert isinstance(tbl.table.c["bar"].type, BIGINT), tbl.table.c["bar"].type
        tbl.create_column_by_example("pippo", "test")
        assert "pippo" in tbl.table.c, tbl.table.c
        assert isinstance(tbl.table.c["pippo"].type, TEXT), tbl.table.c["pippo"].type
        tbl.create_column_by_example("bigbar", 11111111111)
        assert "bigbar" in tbl.table.c, tbl.table.c
        assert isinstance(tbl.table.c["bigbar"].type, BIGINT), tbl.table.c[
            "bigbar"
        ].type
        tbl.create_column_by_example("littlebar", -11111111111)
        assert "littlebar" in tbl.table.c, tbl.table.c
        assert isinstance(tbl.table.c["littlebar"].type, BIGINT), tbl.table.c[
            "littlebar"
        ].type

    def test_key_order(self):
        res = self.db.query("SELECT temperature, place FROM weather LIMIT 1")
        keys = list(res.next().keys())
        assert keys[0] == "temperature"
        assert keys[1] == "place"

    def test_empty_query(self):
        empty = list(self.tbl.find(place="not in data"))
        assert len(empty) == 0, empty


class Constructor(dict):
    """Very simple low-functionality extension to ``dict`` to
    provide attribute access to dictionary contents"""

    def __getattr__(self, name):
        return self[name]


class RowTypeTestCase(unittest.TestCase):
    def setUp(self):
        self.db = connect(row_type=Constructor)
        self.tbl = self.db["weather"]
        for row in TEST_DATA:
            self.tbl.insert(row)

    def tearDown(self):
        for table in self.db.tables:
            self.db[table].drop()

    def test_find_one(self):
        self.tbl.insert(
            {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"}
        )
        d = self.tbl.find_one(place="Berlin")
        assert d["temperature"] == -10, d
        assert d.temperature == -10, d
        d = self.tbl.find_one(place="Atlantis")
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
        x = list(self.tbl.distinct("place"))
        assert len(x) == 2, x
        for item in x:
            assert isinstance(item, Constructor), item
        x = list(self.tbl.distinct("place", "date"))
        assert len(x) == 6, x
        for item in x:
            assert isinstance(item, Constructor), item

    def test_iter(self):
        c = 0
        for row in self.tbl:
            c += 1
            assert isinstance(row, Constructor), row
        assert c == len(self.tbl)


if __name__ == "__main__":
    unittest.main()
