# coding: utf8
from __future__ import unicode_literals
import os
from csv import reader
import unittest
from datetime import datetime
from tempfile import mkdtemp
from shutil import rmtree

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # Python < 2.7 drop-in

from six import PY3, text_type, binary_type
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from dataset import connect
from dataset.util import DatasetException

from .sample_data import TEST_DATA, TEST_CITY_1


class FreezeTestCase(unittest.TestCase):

    def setUp(self):
        self.db = connect('sqlite://')
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)
        self.d = mkdtemp()

    def tearDown(self):
        rmtree(self.d, ignore_errors=True)

    def test_freeze(self):
        from dataset.freeze.app import freeze

        freeze(self.db['weather'].all(), format='csv', filename='wäther.csv'.encode('utf8'), prefix=self.d)
        self.assert_(os.path.exists(os.path.join(self.d, 'wäther.csv')))
        freeze(self.db['weather'].all(), format='csv', filename='wäther.csv', prefix=self.d)
        self.assert_(os.path.exists(os.path.join(self.d, 'wäther.csv')))

    def test_freeze_csv(self):
        from dataset.freeze.app import freeze
        from dataset.freeze.format.fcsv import value_to_str 

        freeze(self.db['weather'].all(), format='csv', filename='weather.csv', prefix=self.d)
        path = os.path.join(self.d, 'weather.csv')
        if PY3:
            fh = open(path, 'rt', encoding='utf8', newline='')
        else:
            fh = open(path, 'rU')
        rows = list(reader(fh))
        keys = rows[0]
        if not PY3:
            keys = [k.decode('utf8') for k in keys]
        for i, d1 in enumerate(TEST_DATA):
            d2 = dict(zip(keys, rows[i + 1]))
            for k in d1.keys():
                v2 = d2[k]
                if not PY3:
                    v2 = v2.decode('utf8')
                v1 = value_to_str(d1[k])
                if not isinstance(v1, text_type):
                    if isinstance(v1, binary_type):
                        v1 = text_type(v1, encoding='utf8')
                    else:
                        v1 = '%s' % v1
                self.assertEqual(v2, v1)

