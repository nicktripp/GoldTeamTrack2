import os
import pickle

from server.indexing.Column import Column, ColumnType
from server.indexing.BTreeIndex import BTreeIndex


class FileIndexer:
    """
    Given a csv file, File Indexer will compute the indexes to all of the columns in the file
    and point you back to them when you ask for them
    """

    def __init__(self, relative_path, table_name, generate=False):
        self.table_name = table_name
        self.input_file = "%s./data/%s.csv" % (relative_path, self.table_name)
        self.output_dir = "%s./data/index/%s/" % (relative_path, self.table_name)
        self.file_indexer_filename = self.output_dir + 'file_indexer_dict'
        self.index_dict = {}
        self.columns = []

        # Make a directory to persist the indices
        self.make_index_directory()

        # Generate an index for each directory
        if generate:
            self.generate_column_indices()
        else:
            self.load_column_indices()

    def make_index_directory(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def generate_column_indices(self):
        size = os.path.getsize(self.input_file)
        with open(self.input_file, 'r') as f:
            # Parse the column names of the csv
            self.columns = Column.get_from_headers(f.readline())

            for j, column in enumerate(self.columns):
                # Get the column value of a row and the position to readline in f
                position = f.tell()
                column_values = []
                column_locations = []
                while position < size:
                    column_values.append(f.readline().split(',')[j])
                    column_locations.append(position)
                    position = f.tell()

                # Reset the file for the next column reading
                f.seek(0)
                f.readline()

                # Don't index something that isn't remotely unique
                assert (len(set(column_values)) > 10)

                # Find out what type of column this is
                column.type = Column.get_type(column_values[0])
                print("Considering column #%d as %s" % (j, column))

                if column.type not in [ColumnType.UNKNOWN, ColumnType.BOOLEAN]:
                    # Compute and store the index
                    index = BTreeIndex(self.output_dir, column.name, column_values, column_locations)
                    self.index_dict[column] = index

        with open(self.file_indexer_filename, 'wb') as f:
            pickle.dump(self.index_dict, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load_column_indices(self):
        with open(self.file_indexer_filename, 'rb') as f:
            self.index_dict = pickle.load(f)

    @staticmethod
    def get_indexer(relative_path, table_name, column_name):
        pickle_file = "%sdata/index/%s/%s" % (relative_path, table_name, column_name)
        print(pickle_file)
        if os.path.isfile(pickle_file):
            print("Loading index from %s" % pickle_file)
            with open(pickle_file, 'rb') as f:
                return pickle.load(f)
        else:
            raise Exception("Index was not found")


    def multi_op(self, keys, comparisons, logic):
        result = None
        for k, c, l in zip(keys, comparisons, logic):
            partial_results = self.op(k, c)
            rows = set()
            for key in partial_results:
                rows = rows.union(partial_results[key])
            if result is None:
                result = rows
            else:
                # Intersection
                if l == 'AND':
                    result = result.intersection(rows)
                # Union
                elif l == 'OR':
                    result = result.union(rows)
        return result


        def read_and_project(self, rows, columns):
        """
        Values of rows are byte location in csv file, ie rows[0] is 0th row to read so f.seek(rows[0]); row = f.readline()
        columns is a list of column names (strings)
        :param rows:
        :param columns:
        :return:
        """

        # TODO: Read first line (headers) figure indices of desired columns


        # TODO: open the csv file (self....)
        with open(self.input_file, 'r') as f:

            # Parse the column names of the csv
            self.columns = Column.get_from_headers(f.readline())

            print("self.columns " + self.columns)

            col_dict = []
            for column in columns:
                col_index = self.columns.index(column)
                col_dict.append({column, col_index})


        # TODO: for each row seek readline extract desired columns
            for row in rows:
                f.seek(row)
                curr_row = f.readline().[:-1].split(',')
            
        # TODO: Reconcatenate each row return as list of csv rows
                final_rows = []
                final_rows.append(columns)
                row_to_add = []
                for col,index in col_dict:
                    row_to_add.append(curr_row[index])
                final_rows.append(row_to_add)