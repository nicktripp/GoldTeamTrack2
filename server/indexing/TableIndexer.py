import os
import pickle
from dateutil.parser import parse as date_parse

from server.indexing import BTreeIndex


class TableIndexer:
    relative_path = "./data/"

    def __init__(self, table, index_class=BTreeIndex.BTreeIndex):
        self._table = table
        self._index_class = index_class
        self._column_indices = {}

        # load or generate indices over the column
        if self._all_indices_exist():
            self._load_indices()
        else:
            self._generate_indices()

        # load or generate the memory locations
        if self._all_mem_locs_exist():
            self._load_mem_locs()
        else:
            self._read_mem_locs()

    def path_for_column(self, col_name):
        return TableIndexer.relative_path + self._table.name + "_" + col_name + ".idx"

    def _all_indices_exist(self):
        for column in self._table.column_index:
            if not os.path.exists(self.path_for_column(column)):
                return False
        return True

    def _load_indices(self):
        for column in self._table.column_index:
            with open(self.path_for_column(column), 'rb') as f:
                self.column_indices[column] = pickle.load(f)

    def _generate_indices(self):
        with open(self._table.filename, 'r') as f:
            size = os.path.getsize(self._table.filename)
            column_names = f.readline()[:-1].split(',')
            start_pos = f.tell()
            for j, col in enumerate(column_names):
                f.seek(start_pos)
                index = self._index_class.make(self._value_location_generator(f, size, j))
                self._column_indices[col] = index
                with open(self.path_for_column(col), 'wb') as g:
                    pickle.dump(index, g)

    def _all_mem_locs_exist(self):
        if not os.path.exists(self.table.filename + "_mem_locs.pickle"):
            return False
        return True

    def _load_mem_locs(self):
        with open(self.table.filename + "_mem_locs.pickle", 'rb') as f:
            self._mem_locs = pickle.load(f)

    def _read_mem_locs(self):
        self._mem_locs = []
        with open(self.table.filename, 'rb') as f:
            size = os.path.getsize(self.table.filename)
            while self._mem_locs[-1] < size:
                f.readline()
                self._mem_locs.append(f.tell())
        with open(self.table.filename + "_mem_locs.pickle", 'wb') as f:
            pickle.dump(self._mem_locs, f, pickle.HIGHEST_PROTOCOL)

    def _value_location_generator(self, f, size, j):
        position = f.tell()
        i = 0
        while position < size:
            col_loc = position
            col_val = f.readline().split(',')[j]
            position = f.tell()
            yield col_val, col_loc, i
            i += 1

    @property
    def table(self):
        return self._table

    @property
    def column_indices(self):
        return self._column_indices

    @property
    def mem_locs(self):
        return self._mem_locs

    @staticmethod
    def parse_value(val):
        try:
            return int(val)
        except ValueError:
            pass

        try:
            return float(val)
        except ValueError:
            pass

        if val == 'True' or val == 'true':
            return True
        elif val == 'False' or val == 'false':
            return False

        try:
            return date_parse(val)
        except ValueError:
            pass

        # Treat the right as TEXT
        return val
