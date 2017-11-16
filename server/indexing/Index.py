from server.indexing.BTree import BTree
from server.indexing.Bitmap import Bitmap


class Index:
    """
    Index is an indexing for a single column F in a CSV file D.

    A Bitmap index will be computed over each F in D. The Bitmap will be used for very efficient partial match querying.

    A B+ Tree acts as the secondary indexing technique for mapping from unique values of F to bit-vectors in the
    bitmap indexing.

    A B+ Tree acts as the secondary indexing technique for mapping a kth record to its location in the csv.
    """

    def __init__(self, index_directory, index_name, column_values, column_locations):
        # Determines were to write index to disk
        self.index_directory = index_directory
        self.index_name = index_name

        # Compute Bitmap index and its B+ Tree record lookup
        self.bitmap = Bitmap(column_values)

        # Fill the B+ Tree record location lookup
        self.record_location = BTree()
        self.fill_record_location_lookup(column_locations)


    def fill_record_location_lookup(self, record_locations):
        # TODO: Map record number to record_location
        pass

