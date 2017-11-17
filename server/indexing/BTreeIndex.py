from server.indexing.BTree import BTree
import pickle

class BTreeIndex:

    def __init__(self, index_directory, index_name, column_values, column_locations):
        # Determines were to write index to disk
        self.index_directory = index_directory
        self.index_name = index_name

        # Initialize the BTree index
        initial_values = {}
        for cv, cl in zip(column_values[:11], column_locations[:11]):
            if cv not in initial_values:
                initial_values[cv] = {cl}
        self.btree = BTree(10, initial_values)
        print(initial_values)

        # Finish filling the BTree Index
        for value, location in zip(column_values[11:], column_locations[11:]):
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