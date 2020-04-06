from datetime import datetime, date

from sqlalchemy import Integer, UnicodeText, Float, BigInteger
from sqlalchemy import Boolean, Date, DateTime, Unicode, JSON
from sqlalchemy.dialects.postgresql import JSONB
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

    def __init__(self, dialect=None):
        self._dialect = dialect

    @property
    def json(self):
        if self._dialect is not None and self._dialect == 'postgresql':
            return JSONB
        return JSON

    def guess(self, sample):
        """Given a single sample, guess the column type for the field.

        If the sample is an instance of an SQLAlchemy type, the type will be
        used instead.
        """
        if isinstance(sample, TypeEngine):
            return sample
        if isinstance(sample, bool):
            return self.boolean
        elif isinstance(sample, int):
            return self.bigint
        elif isinstance(sample, float):
            return self.float
        elif isinstance(sample, datetime):
            return self.datetime
        elif isinstance(sample, date):
            return self.date
        elif isinstance(sample, dict):
            return self.json
        return self.text
