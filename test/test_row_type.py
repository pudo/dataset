from datetime import datetime

from .conftest import TEST_CITY_1


class Constructor(dict):
    """Very simple low-functionality extension to ``dict`` to
    provide attribute access to dictionary contents"""

    def __getattr__(self, name):
        return self[name]


def test_find_one(db, table):
    db.row_type = Constructor
    table.insert({"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"})
    d = table.find_one(place="Berlin")
    assert d["temperature"] == -10, d
    assert d.temperature == -10, d
    d = table.find_one(place="Atlantis")
    assert d is None, d


def test_find(db, table):
    db.row_type = Constructor
    ds = list(table.find(place=TEST_CITY_1))
    assert len(ds) == 3, ds
    for item in ds:
        assert isinstance(item, Constructor), item
    ds = list(table.find(place=TEST_CITY_1, _limit=2))
    assert len(ds) == 2, ds
    for item in ds:
        assert isinstance(item, Constructor), item


def test_distinct(db, table):
    db.row_type = Constructor
    x = list(table.distinct("place"))
    assert len(x) == 2, x
    for item in x:
        assert isinstance(item, Constructor), item
    x = list(table.distinct("place", "date"))
    assert len(x) == 6, x
    for item in x:
        assert isinstance(item, Constructor), item


def test_iter(db, table):
    db.row_type = Constructor
    c = 0
    for row in table:
        c += 1
        assert isinstance(row, Constructor), row
    assert c == len(table)
