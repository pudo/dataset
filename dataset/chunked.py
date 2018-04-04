

class ChunkedInsert(object):
    """Batch up insert operations
    with ChunkedStorer(my_table) as storer:
        table.insert(row)

    Rows will be inserted in groups of 1000
    """

    def __init__(self, table, chunksize=1000):
        self.queue = []
        self.table = table
        self.chunksize = chunksize

    def insert(self, item):
        self.queue.append(item)
        if len(self.queue) > self.chunksize:
            self.table.insert_many(self.queue)
            self.queue = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.table.insert_many(self.queue)
        self.queue = []
