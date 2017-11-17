from server.indexing.BTree import BTree
import pickle
import os
import time
import sys

sys.setrecursionlimit(10000)


class BTreeIndex:

    def __init__(self, index_directory, index_name, column_values, column_locations, blocksize=10):
        # Determines were to write index to disk
        self.index_directory = index_directory
        self.index_name = index_name

        # Initialize the BTree index
        initial_values = {}
        count = 0
        for cv, cl in zip(column_values, column_locations):
            count += 1
            if cv not in initial_values:
                initial_values[cv] = {cl}
            if len(initial_values) > blocksize:
                break
        self.btree = BTree(blocksize, initial_values)

        # Finish filling the BTree Index
        for value, location in zip(column_values[count:], column_locations[count:]):
            result = self.btree.lookup(value)
            if result[0]:
                result[1].add(location)
            else:
                self.btree.insert(value, {location})

        # Save the Index
        with open(index_directory + index_name, 'wb') as f:
            pickle.dump(self.btree, f, pickle.HIGHEST_PROTOCOL)

    def where(self, value):
        with open(self.index_directory + self.index_name, 'rb') as f:
            index = pickle.load(f)
        return index.lookup(value)


if __name__ == "__main__":
    # Make a directory to persist the indices
    pickle_dir = '../data/tmp'
    if not os.path.exists(pickle_dir):
        os.makedirs(pickle_dir)

    # Make an index for column of the CSV
    filename = '../../data/movies.csv'
    size = os.path.getsize(filename)
    with open(filename, 'r') as f:
        headers = f.readline()
        for j, header in enumerate(headers.split(',')):
            print("Starting to index column #%d %s" % (j, header))
            t0 = time.time()

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
            BTreeIndex(pickle_dir, header, column_values, column_locations)
            print("Spent %.3f s" % (time.time() - t0,))
