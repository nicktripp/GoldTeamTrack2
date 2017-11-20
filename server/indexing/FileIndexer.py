import os

from server.indexing import Column
from server.indexing.BTreeIndex import BTreeIndex


class FileIndexer:
    """
    Given a csv file, File Indexer will compute the indexes to all of the columns in the file
    and point you back to them when you ask for them
    """

    def __init__(self, input_file, output_dir):
        self.input_file = input_file

        # Make a directory to persist the indices
        pickle_dir = '../../data/index/'
        if not os.path.exists(pickle_dir):
            os.makedirs(pickle_dir)

        # Make an index for column of the CSV
        size = os.path.getsize(input_file)
        self.index_dict = {}
        with open(input_file, 'r') as f:
            # Parse the column names of the csv
            self.columns = Column.get_from_headers(f.readline())
            for j, column in enumerate(self.columns):
                print("Starting to index column #%d %s" % (j, column))

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
                assert(len(set(column_values)) > 10)

                # Find out what type of column this is
                column.type = Column.get_type(column_values)

                # Compute and store the index
                index = BTreeIndex(pickle_dir, column.name, column_values, column_locations)
                self.index_dict[column] = index

