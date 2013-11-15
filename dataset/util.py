#coding: utf-8
import re
from unicodedata import normalize as ucnorm, category

SLUG_REMOVE = re.compile(r'[,\s\.\(\)/\\;:]*')

class DatasetException(Exception):
    pass

class FreezeException(DatasetException):
    pass

