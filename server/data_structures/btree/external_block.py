import math
from itertools import repeat

from server.data_structures.btree.block import Block


class ExternalBlock(Block):
    """
    ExternalBlock is a leaf node in a B+ Tree
    """

    def __init__(self, size, next_leaf=None):
        super().__init__(size)
        self.next_leaf = next_leaf

    def __repr__(self):
        return "{External %s :: %s}" % (self.keys, self.values)

    def indentedToString(self, indents):
        return '\t' * indents + str(self)

    def __getitem__(self, key):
        """
        Searches for the key in the Block's keys, returning the value at the same index or None
        :param key:
        :return:
        """
        for i, ki in enumerate(self.keys):
            if ki == key:
                return self.values[i]
        return None

    def insert_recurse(self, key, value):
        """
        Stops the recursion through the InternalBlocks by inserting into self
        :param key:
        :param value:
        :return:
        """
        return self.insert(key, value)

    def insert_with_split(self, key, value):
        # Insert the key-value pair, then split the leaf
        inserted = False
        for i, ki in enumerate(self.keys):
            if key < ki:
                self.keys.insert(i, key)
                self.values.insert(i, value)
                inserted = True
                break

        if not inserted:
            self.keys.append(key)
            self.values.append(value)

        # Move pairs to a new block
        split = math.ceil(len(self.keys) / 2)
        right = ExternalBlock(self.size)
        right.keys = self.keys[-split:]
        right.values = self.values[-split:]

        # Remove pairs from current block
        keep = math.floor(len(self.keys) / 2)
        del self.keys[keep:]
        del self.values[keep:]

        # Reorder the leaf pointers
        right.next_leaf = self.next_leaf
        self.next_leaf = right

        # Assert that things were done correctly
        assert (len(self.keys) + len(right.keys) == self.size + 1)
        assert (len(self.values) + len(right.values) == self.size + 1)
        keys = self.keys + right.keys
        for i in range(1, len(keys)):
            assert(keys[i-1] < keys[i])

        # Return median key and right block
        return right.keys[0], right

    def insert_without_split(self, key, value):
        # Insert directly into the leaf
        for i, ki in enumerate(self.keys):
            if key < ki:
                # Insert the key and value
                self.keys.insert(i, key)
                self.values.insert(i, value)
                return None

        # key is largest key in block
        self.keys.append(key)
        self.values.append(value)

        # Assert that we didn't need to split
        assert(len(self.keys) <= self.size)
        assert(len(self.values) <= self.size)
        return None
