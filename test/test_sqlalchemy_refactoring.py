import unittest
from datetime import datetime

from dataset import connect

from .sample_data import TEST_DATA

POSTGRES_URL = ""


class SQLAlchemyTestCasePostgreSQL(unittest.TestCase):
    def setUp(self):
        self.db = connect(POSTGRES_URL)
        self.tbl = self.db["weather"]
        for row in TEST_DATA:
            self.tbl.insert(row)

    def tearDown(self):
        # self.tbl.drop()
        pass

    def test_insert(self):
        last_id = self.tbl.insert(
            {"date": datetime(2011, 1, 2), "temperature": -10, "place": "Berlin"}
        )

        assert self.tbl.find_one(id=last_id)["place"] == "Berlin"

    def test_insert_and_change_schema(self):
        last_id = self.tbl.insert(
            {"date": datetime(2022, 3, 4), "temperature": 22, "place": "Brașov"}
        )
        
        assert self.tbl.find_one(id=last_id)["place"] == "Brașov"

        last_id = self.tbl.insert(
            {"date": datetime(2022, 3, 4), "temperature": 22, "place": "Brașov", "feels_like": 20}
        )

        # assert self.tbl.find_one(id=last_id)["feels_like"] == 20
