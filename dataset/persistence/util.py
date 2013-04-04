from datetime import datetime

from sqlalchemy import Integer, UnicodeText, Float, DateTime, Boolean


def guess_type(sample):
    if isinstance(sample, bool):
        return Boolean
    elif isinstance(sample, int):
        return Integer
    elif isinstance(sample, float):
        return Float
    elif isinstance(sample, datetime):
        return DateTime
    return UnicodeText


class ResultIter(object):
    """ SQLAlchemy ResultProxies are not iterable to get a 
    list of dictionaries. This is to wrap them. """

    def __init__(self, rp):
        self.rp = rp
        self.count = rp.rowcount
        self.keys = self.rp.keys()

    def next(self):
        row = self.rp.fetchone()
        if row is None:
            raise StopIteration
        return dict(zip(self.keys, row))

    def __iter__(self):
        return self


