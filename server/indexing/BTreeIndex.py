from server.data_structures.btree.btree import BTree
import pickle
import os
import time
import sys

sys.setrecursionlimit(10000)


class BTreeIndex:

    def __init__(self, index_directory, index_name, column_values, column_locations, block_size=10):
        # Determines were to write index to disk
        self.index_directory = index_directory
        self.index_name = index_name

        # Initialize the BTree index with 3 unique key-value pairs
        initial_values = self.get_initial_values(column_values, column_locations)
        self.btree = BTree(block_size, initial_values)

        # Insert the rest of the pairs
        for cv, cl in zip(column_values, column_locations):
            # print("Inserting %s" % ((cv, cl),))
            lookup = self.btree[cv]
            if lookup:
                lookup.add(cl)
            else:
                self.btree[cv] = {cl}

        # Save the Index
        with open(index_directory + index_name, 'wb') as f:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

    def get_initial_values(self, keys, values):
        assert(len(keys) == len(values))
        initial_values = {}
        i = 0
        n = len(keys)
        while len(initial_values) < 3 and i < n:
            initial_values[keys[i]] = {values[i]}
            i += 1
        assert(len(initial_values) == 3)
        return initial_values


    def where(self, key):
        with open(self.index_directory + self.index_name, 'rb') as f:
            index = pickle.load(f)
        return index[key]

    # TODO: Add a switch or something that points the op to the correct comparison
    def op(self, key, comparison):
        """
        The key will be compared against all of the keys in the index with the provided comparison

        <, >, <>, =, etc.
        :param key:
        :param comparison:
        :return: set of row locations in file
        """
        return None

    def lessThan(self, key):
        pass



if __name__ == "__main__":
    # Make a directory to persist the indices
    pickle_dir = '../../data/tmp/'
    if not os.path.exists(pickle_dir):
        os.makedirs(pickle_dir)

    # Make an index for column of the CSV
    filename = '../../data/movies.csv'
    size = os.path.getsize(filename)
    with open(filename, 'r') as f:
        headers = f.readline()
        for j, header in enumerate(headers.split(',')):
            print("Starting to index column #%d %s" % (j, header))

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

            # Initialize and save a pickle dump of the index
            if len(set(column_values)) >= 10:
                index = BTreeIndex(pickle_dir, header, column_values, column_locations)
