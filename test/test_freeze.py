# -*- encoding: utf-8 -*-
import unittest
from dataset.freeze.format.fcsv import value_to_str

from .sample_data import TEST_DATA


class FreezeTestCase(unittest.TestCase):

    def test_value_to_str1(self):
        assert '2011-01-01T00:00:00' == value_to_str(TEST_DATA[0]['date'])

    def test_value_to_str2(self):
        assert 'hóla' == value_to_str(u'\u0068\u00f3\u006c\u0061')

    def test_value_to_str3(self):
        assert '' == value_to_str(None)
