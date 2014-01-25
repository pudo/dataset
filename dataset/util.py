#coding: utf-8
import re

SLUG_REMOVE = re.compile(r'[,\s\.\(\)/\\;:]*')


class DatasetException(Exception):
    pass


class FreezeException(DatasetException):
    pass
