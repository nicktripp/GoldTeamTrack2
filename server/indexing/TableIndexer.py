import os


class TableIndexer:

    relative_path = "./data/"

    def __init__(self, table):
        self._table = table

        # load or generate indices over the column
        if os.path.exists(TableIndexer.relative_path + self._table.name + ".idx"):
            self._load_indices()
        else:
            self._generate_indices()