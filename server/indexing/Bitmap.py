from server.indexing.BTree import BTree


class Bitmap:
    """
    Implementation of Bitmap Index with B+ Tree as secondary indexing technique
    """

    def __init__(self, values):
        # Initialize secondary indexes
        self.bitvector_lookup = BTree()

        # Encode and Compress the values as bit vectors
        bitvectors = self.encode(values)
        compressed = self.compress(bitvectors)

        # Fill the secondary index
        self.fill_lookup(values, compressed)


    def encode(self, values):
        # TODO: Encode values as bit vectors
        pass

    def fill_lookup(self, values, compressed):
        # TODO: Map row values to compressed bit vectors by index in list
        pass

    @staticmethod
    def compress(bitvectors):
        # TODO: Compress the bitvectors using Run-Length Encoding
        pass

    @staticmethod
    def decompress(compressed):
        # TODO: Decompress compressed bitvectors
        pass
