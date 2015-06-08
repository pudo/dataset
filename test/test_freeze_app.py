# coding: utf-8
"""
Test CLI following the recipe at http://dustinrcollins.com/testing-python-command-line-apps
"""
import os
import unittest
from tempfile import mkdtemp
from shutil import rmtree
from copy import copy

from six import StringIO

from dataset import connect
from dataset.util import FreezeException
from dataset.freeze.config import Configuration, Export
from dataset.freeze.app import create_parser, freeze_with_config, freeze_export
from .sample_data import TEST_DATA


class FreezeAppTestCase(unittest.TestCase):
    """
    Base TestCase class, sets up a CLI parser
    """
    def setUp(self):
        parser = create_parser()
        self.parser = parser
        self.d = mkdtemp()
        self.db_path = os.path.abspath(os.path.join(self.d, 'db.sqlite'))
        self.db = 'sqlite:///' + self.db_path
        _db = connect(self.db)
        tbl = _db['weather']
        for i, row in enumerate(TEST_DATA):
            _row = copy(row)
            _row['count'] = i
            _row['bool'] = True
            _row['none'] = None
            tbl.insert(_row)

    def tearDown(self):
        rmtree(self.d, ignore_errors=True)

    def test_with_config(self):
        cfg = Configuration(os.path.join(os.path.dirname(__file__), 'Freezefile.yaml'))
        cfg.data['common']['database'] = self.db
        cfg.data['common']['prefix'] = self.d
        cfg.data['common']['query'] = 'SELECT * FROM weather'
        cfg.data['exports'] = [
            {'filename': '{{identity:count}}.json', 'mode': 'item', 'transform': {'bool': 'identity'}},
            {'filename': 'weather.json', 'format': 'tabson'},
            {'filename': 'weather.csv', 'fileobj': StringIO(), 'format': 'csv'},
            {'filename': 'weather.json', 'fileobj': StringIO(), 'format': 'tabson'},
            {'filename': 'weather.json', 'format': 'tabson', 'callback': 'read'},
            {'skip': True}]
        freeze_with_config(cfg, db=self.db)
        self.assertRaises(FreezeException, freeze_export, Export(cfg.data['common'], {'query': 'SELECT * FROM notable'}))

    def test_unicode_path(self):
        cfg = Configuration(os.path.join(os.path.dirname(__file__), 'Freezefile.yaml'))
        cfg.data['common']['database'] = self.db
        cfg.data['common']['prefix'] = os.path.join(self.d, u'über')
        cfg.data['common']['query'] = 'SELECT * FROM weather'
        cfg.data['exports'] = [{'filename': 'weather.csv', 'format': 'csv'}]
        freeze_with_config(cfg, db=self.db)


if __name__ == '__main__':
    unittest.main()
