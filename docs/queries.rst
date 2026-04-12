
.. _advanced_filters:

Advanced filters
================

``dataset`` provides two methods for running queries: :py:meth:`table.find() <dataset.Table.find>`
and :py:meth:`db.query() <dataset.Database.query>`. The table find helper method provides 
limited, but simple filtering options::

    results = table.find(column={operator: value})
    # e.g.:
    results = table.find(name={'like': '%mole rat%'})

A special form is using keyword searches on specific columns::

    results = table.find(value=5)
    # equal to:
    results = table.find(value={'=': 5})

    # Lists, tuples and sets are turned into `IN` queries:
    results = table.find(category=('foo', 'bar'))
    # equal to:
    results = table.find(value={'in': ('foo', 'bar')})

The following comparison operators are supported:

============== ============================================================
Operator       Description
============== ============================================================
gt, >          Greater than
lt, <          Less than
gte, >=        Greater or equal
lte, <=        Less or equal
!=, <>, not    Not equal to a single value
in             Value is in the given sequence
notin          Value is not in the given sequence
like, ilike    Text search, ILIKE is case-insensitive. Use ``%`` as a wildcard
notlike        Like text search, except check if pattern does not exist
between, ..    Value is between two values in the given tuple
startswith     String starts with
endswith       String ends with
============== ============================================================

Querying for a specific value on a column that does not exist on the table
will return no results.

You can also pass `SQLAlchemy core expressions`_ directly into the
:py:meth:`table.find() <dataset.Table.find>` method as positional arguments.
Access the underlying SQLAlchemy table via ``table.table`` and its columns
via ``table.table.columns``::

    from sqlalchemy import or_

    # Get a column object:
    city = table.table.columns.city
    # Use a SQLAlchemy clause:
    results = table.find(city.ilike('amsterda%'))

    # Combine with OR:
    country = table.table.columns.country
    results = table.find(or_(city == 'Amsterdam', country == 'Germany'))

    # Combine SQLAlchemy clauses with keyword filters:
    results = table.find(city.ilike('new%'), country='US')

These clauses also work with :py:meth:`table.count() <dataset.Table.count>`,
:py:meth:`table.find_one() <dataset.Table.find_one>`, and
:py:meth:`table.delete() <dataset.Table.delete>`.

Queries using raw SQL
---------------------

To run more complex queries with JOINs, or to perform GROUP BY-style
aggregation, you can also use :py:meth:`db.query() <dataset.Database.query>`
to run raw SQL queries instead. This also supports parameterisation to avoid
SQL injections::

    statement = 'SELECT user, COUNT(*) c FROM photos GROUP BY user'
    for row in db.query(statement):
        print(row['user'], row['c'])

    # With parameter binding:
    results = db.query('SELECT * FROM users WHERE age > :min_age', min_age=21)

For fully programmatic, composable query building, consider using
`SQLAlchemy core expressions`_ directly.

.. _SQLAlchemy core expressions: https://docs.sqlalchemy.org/en/latest/core/tutorial.html#selecting