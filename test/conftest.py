import pytest
from datetime import datetime

import dataset


TEST_CITY_1 = "B€rkeley"
TEST_CITY_2 = "G€lway"

TEST_DATA = [
    {"date": datetime(2011, 1, 1), "temperature": 1, "place": TEST_CITY_2},
    {"date": datetime(2011, 1, 2), "temperature": -1, "place": TEST_CITY_2},
    {"date": datetime(2011, 1, 3), "temperature": 0, "place": TEST_CITY_2},
    {"date": datetime(2011, 1, 1), "temperature": 6, "place": TEST_CITY_1},
    {"date": datetime(2011, 1, 2), "temperature": 8, "place": TEST_CITY_1},
    {"date": datetime(2011, 1, 3), "temperature": 5, "place": TEST_CITY_1},
]


@pytest.fixture(scope="function")
def db():
    db = dataset.connect()
    yield db
    db.close()


@pytest.fixture(scope="function")
def table(db):
    tbl = db["weather"]
    tbl.delete()
    tbl.insert_many(TEST_DATA)
    yield tbl
