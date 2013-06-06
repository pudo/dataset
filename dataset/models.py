class Model(object):
    """
    Simple Model class to work with datasets and objects.

    Declare your class using this as superclass:

    >>> class Person(Model):
    >>>     _table = 'persons'
    >>>     _fields = [
    >>>         ('name', None),
    >>>         ('age', 18),
    >>>         ('location', 'Madrid'),
    >>>     ]
    >>>     _indexes = ['name']

    :code:`_table` : The table name of the model.

    :code:`_fields` : List of tuples with two values: field name
    and field default value.

    :code:`_indexes` : List of fields that have an index.

    For creating an object, declare the :code:`_engine` kwarg as your
    database engine, and the class will select the table connection
    according to its :code:`_table` variable.

    >>> engine = connect('sqlite:///:memory:')
    >>> item = Person(_engine=engine)

    """
    _primary_key = 'id'
    _table = ''
    _fields = []  # List of tuples (field, field default value)
    _indexes = []  # List of fields

    def __init__(self, _engine, *args, **kwargs):
        # Calling superclass __init__ (stablishes ddbb connection)
        self._engine = _engine
        self._connection = self._engine[self._table]

        # Check if table is set
        if not self._table:
            raise Exception('%s _table parameter is not defined!' % (
                            self.__class__.__name__)
                            )

        if not len(self._fields):
            raise Exception('%s _fields parameter is empty!' % (
                            self.__class__.__name__)
                            )

        # Create table indexes
        if len(self._indexes):
            self._connection.create_index(self._indexes)

        # Set object fields according with the configured fields
        for field in self._fields + [(self._primary_key, None)]:
            value = field[1]
            if field[0] in kwargs:
                value = kwargs[field[0]]
            setattr(self, field[0], value)

    def save(self):
        """
        Saves the model, using an INSERT if primary_key is None or an
        UPDATE if there's a primary key set.
        ::

            item = MyModel(_engine=engine)
            item.foo = 'var'
            item.save()

        ..
        """
        data = dict()
        for field in self._fields:
            data[field[0]] = getattr(self, field[0])

        if getattr(self, self._primary_key) is not None:
            # Update (primary key is set)
            data[self._primary_key] = getattr(self, self._primary_key)
            self._connection.update(data, [self._primary_key])
        else:
            # Insert
            row_pk = self._connection.insert(data)
            setattr(self, self._primary_key, row_pk)

    def delete(self):
        """
        Deletes the row and reinitializes the model.
        ::

            item = MyModel(_engine=engine)
            item.foo = 'var'
            item.save()

            item.delete()  # Model is now reset and ddbb record is removed

        ..
        """
        try:
            object_pk = getattr(self, self._primary_key)
            data = {
                self._primary_key: object_pk
            }
            self._connection.delete(**data)
            self.__init__(_engine=self._engine)  # Reset model
        except AttributeError:
            pass

    def find(self, *args, **kwargs):
        """
        Return objects with the matching kwargs, if none found,
        raises Model.NotFound
        ::

            items = MyModel(_engine=engine).find(foo='bar')

        ..
        """
        result = []
        items = self._connection.find(*args, **kwargs)
        for item in items:
            obj = self.__class__(_engine=self._engine, **item)
            result.append(obj)

        if not len(result):
            raise self.NotFound()

        return result

    def find_one(self, *args, **kwargs):
        """
        Return first object with the matching kwargs, if none found,
        raises Model.NotFound
        ::

            item = MyModel(_engine=engine).find_one(foo='bar')

        ..
        """
        result = None
        item = self._connection.find_one(**kwargs)
        if item:
            result = self.__class__(_engine=self._engine, **item)
        else:
            raise self.NotFound()
        return result

    def __unicode__(self):
        return "<Model: %s>" % self.__class__.__name__

    @staticmethod
    class NotFound(Exception):
        pass
