

class ChunkedInsert(object):
    """Batch up insert operations
    with ChunkedStorer(my_table) as storer:
        table.insert(row)

    Rows will be inserted in groups of 1000
    """

    def __init__(self, table, chunksize=1000):
        self.queue = []
        self.fields = set()
        self.table = table
        self.chunksize = chunksize

    def flush(self):
        for item in self.queue:
            for field in self.fields:
                item[field] = item.get(field)
        self.table.insert_many(self.queue)
        self.queue = []

    def insert(self, item):
        self.fields.update(item.keys())
        self.queue.append(item)
        if len(self.queue) >= self.chunksize:
            self.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
