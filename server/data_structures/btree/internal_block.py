import math

from server.data_structures.btree.block import Block


class InternalBlock(Block):
    """
    InternalBlock is a Node in the B+ Tree that has children
    (either InternalBlock children or ExternalBlock children not both)
    """
    def __init__(self, size):
        super().__init__(size)

    def __repr__(self):
        return "{Internal %s :: [\n%s\n]}" % (self.keys, '\n'.join([v.indentedToString(1) for v in self.values]))

    def indentedToString(self, indents):
        delimiter = '\t' * indents + '\n'
        tabs = '\t' * indents
        children = delimiter.join([v.indentedToString(indents + 1) for v in self.values])
        return "%sInternal %s :: [\n%s\n%s]}" % (tabs, self.keys, children, tabs)

    def __getitem__(self, key):
        """
        Searches for an item in one of the children
        :param key:
        :return: value
        """
        # Search before first key
        if key < self.keys[0]:
            return self.values[0][key]
        # Search after last key
        elif self.keys[-1] <= key:
            return self.values[-1][key]
        # Search in between keys
        else:
            for i in range(1, len(self.keys)):
                if self.keys[i-1] <= key < self.keys[i]:
                    return self.values[i][key]

    def insert_recurse(self, key, value):
        """
        Inserts the key value pair into a child. If the child splits, then
        the median key from the split is inserted into this block, with the
        value of the right block of the split
        :param key:
        :param value:
        :return:
        """
        insert_result = None

        # Insert before first key
        if key < self.keys[0]:
            insert_result = self.values[0].insert_recurse(key, value)
        # Insert after the last key
        elif self.keys[-1] <= key:
            insert_result = self.values[-1].insert_recurse(key, value)
        else:
            for i in range(1, len(self.keys)):
                if self.keys[i-1] <= key < self.keys[i]:
                    insert_result = self.values[i].insert_recruse(key, value)

        # Check if insertion resulted in split
        if insert_result is None:
            return None

        # A split occurred, insert the new key value pair into the current block
        return self.insert(*insert_result)

    def insert_with_split(self, key, value):
        """
        A split needs to occur while inserting into this internal block
        :param key:
        :param value:
        :return:
        """
        # Insert the key value pair into the current pairs
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

        # Move the last floor(n/2) keys to the right
        move = math.ceil(self.size / 2)
        right = InternalBlock(self.size)
        right.keys = self.keys[-move:]

        # Use the middle key as the split key
        keep = math.floor(self.size / 2)
        median_key = self.keys[keep]

        # Remove the median key and right keys from the left
        del self.keys[keep:]

        # Move the last floor((n+2)/2) values to the right
        move = math.floor((self.size + 2) / 2)
        right.values = self.values[-move:]

        # Remove the moved values from self.values
        keep = math.ceil((self.size + 2) / 2)
        del self.values[keep:]

        # Assert that things were done correctly
        assert(len(self.keys) + 1 == len(self.values))
        assert(len(right.keys) + 1 == len(right.values))
        assert(len(self.keys) + len(right.keys) == self.size)
        assert(len(self.values) + len(right.values) == self.size + 2)
        keys = self.keys + [median_key] + right.keys
        for i in range(1, len(keys)):
            assert(keys[i-1] < keys[i])

        # Return the split key and the right block
        return median_key, right

    def insert_without_split(self, key, value):
        # Insert in between keys
        for i in range(1, len(self.keys)):
            if self.keys[i-1] <= key < self.keys[i]:
                self.keys.insert(i, key)
                self.values.insert(i + 1, value)
                return None

        # key is larger than all other keys in block
        self.keys.append(key)
        self.values.append(value)

        # Assert that we didn't need to split
        assert(len(self.keys) <= self.size)
        assert(len(self.values) <= self.size + 1)
        assert(len(self.keys) + 1 == len(self.values))
        return None
