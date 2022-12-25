
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

You can also pass additional SQLAlchemy clauses into the :py:meth:`table.find() <dataset.Table.find>` method
by falling back onto the SQLAlchemy core objects wrapped by `dataset`::

    # Get the column `city` from the dataset table:
    column = table.table.columns.city
    # Define a SQLAlchemy clause:
    clause = column.ilike('amsterda%')
    # Query using the clause:
    results = table.find(clause)

This can also be used to define combined OR clauses if needed (e.g. `city = 'Bla' OR country = 'Foo'`).

Queries using raw SQL
---------------------

To run more complex queries with JOINs, or to perform GROUP BY-style
aggregation, you can also use :py:meth:`db.query() <dataset.Database.query>`
to run raw SQL queries instead. This also supports parameterisation to avoid
SQL injections.

Finally, you should consider falling back to SQLAlchemy_ core to construct
queries if you are looking for a programmatic, composable method of generating
SQL in Python.

.. _SQLALchemy: https://docs.sqlalchemy.org/
