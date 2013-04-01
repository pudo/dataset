import unittest
from datetime import datetime

from dataset import connect

class DatabaseTestCase(unittest.TestCase):

    def setUp(self):
        self.db = connect('sqlite:///:memory:')

    def test_create_table(self):
        table = self.db['foo']
        assert table.table.exists()
        assert len(table.table.columns) == 1, table.table.columns


if __name__ == '__main__':
    unittest.main()

