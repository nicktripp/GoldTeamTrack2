import os
import pickle

from server.indexing import BTreeIndex
from server.query.TableProjector import TableProjector
from server.query.Table import Table


class TableIndexer:
    relative_path = "./data/"

    def __init__(self, table, cols_to_load, index_class):
        self._table = table
        self._index_class = index_class
        self._column_indices = {}

        # load or generate indices over the column
        if self._all_indices_exist(cols_to_load):
            self._load_indices(cols_to_load)
        else:
            self._generate_indices(cols_to_load)

        # generate col_types if they don't exist
        if not self.table.col_types_exist():
            self.table.read_col_types()

        # load or generate the memory locations
        if self._all_mem_locs_exist():
            self._load_mem_locs()
        else:
            self._read_mem_locs()



    def path_for_column(self, col_name):
        return TableIndexer.relative_path + self._table.name + "_" + col_name + ".idx"

    def _all_indices_exist(self, cols_to_load):
        for column in cols_to_load:
            if not os.path.exists(self.path_for_column(column.name)):
                return False
        return True

    def _load_indices(self, cols_to_load):
        for column in cols_to_load:
            column = column.name
            with open(self.path_for_column(column), 'rb') as f:
                self.column_indices[column] = pickle.load(f)

    def _generate_indices(self, cols_to_load):
        with open(self._table.filename, encoding='utf8') as f:
            size = os.path.getsize(self._table.filename)
            column_names = f.readline()[:-1].split(',')
            to_load = set([c.name for c in cols_to_load])
            start_pos = f.tell()
            for j, col in enumerate(column_names):
                if col not in to_load:
                    continue
                elif os.path.exists(self.path_for_column(col)):
                    with open(self.path_for_column(col)) as f:
                        self._column_indices[col] = pickle.load(f)
                        continue
                f.seek(start_pos)
                index = self._index_class.make(self._value_location_generator(f, size, j), self._table, col)
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
        with open(self.table.filename, encoding='utf8') as f:
            size = os.path.getsize(self.table.filename)
            self._mem_locs = [col_loc for col_val, col_loc, rec_num in self._value_location_generator(f, size, 0)][1:]

        with open(self.table.filename + "_mem_locs.pickle", 'wb') as f:
            pickle.dump(self._mem_locs, f, pickle.HIGHEST_PROTOCOL)

    def _value_location_generator(self, f, size, j):
        position = f.tell()
        i = 0
        while position < size:
            col_loc = position
            col_vals = TableProjector.read_col_vals_multiline(col_loc, f)
            col_val = col_vals[j]
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
