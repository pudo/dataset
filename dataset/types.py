from datetime import datetime, date

from sqlalchemy import Integer, UnicodeText, Float, BigInteger
from sqlalchemy import Boolean, Date, DateTime, Unicode
from sqlalchemy.types import TypeEngine


class Types(object):
    """A holder class for easy access to SQLAlchemy type names."""
    integer = Integer
    string = Unicode
    text = UnicodeText
    float = Float
    bigint = BigInteger
    boolean = Boolean
    date = Date
    datetime = DateTime

    def guess(cls, sample):
        """Given a single sample, guess the column type for the field.

        If the sample is an instance of an SQLAlchemy type, the type will be
        used instead.
        """
        if isinstance(sample, TypeEngine):
            return sample
        if isinstance(sample, bool):
            return cls.boolean
        elif isinstance(sample, int):
            return cls.bigint
        elif isinstance(sample, float):
            return cls.float
        elif isinstance(sample, datetime):
            return cls.datetime
        elif isinstance(sample, date):
            return cls.date
        return cls.text
