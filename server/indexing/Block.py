import numpy as np
import math


class Block:
    """
    A Block is either an internal or leaf node in a B+ Tree. All Blocks will have the same number of keys as
    defined by the static field member Block.size. Block.size also determines the number of values for a Block
    (Block.size + 1)
    """

    def __repr__(self):
        if self.leaf:
            return "{Leaf %s: %s}\n" % (self.keys, self.values)
        else:
            return "{Internal %s: %s}\n" % (self.keys, self.values)

    def __init__(self, size, is_leaf=True):
        """
        Initializes a Block
        The Block is assumed to be a Leaf unless told otherwise
        """
        self.size = size
        self.leaf = is_leaf
        self.keys = np.array([None] * self.size)
        self.values = np.array([None] * (self.size + 1))

    def get_value(self, key):
        """
        Lookups up a value for a key

        A key, K, identifies a value, V, for a block with keys K_1, K_2, ..., K_n and values V_1, V_2, ..., V_(n+1).

        If self is a leaf, then K must match K_i for i in 1 to n.
        If a match succeeds for a leaf (True, V) is returned.
        If a match fails for a leaf (False, V_(n+1)) is returned.

        If self is not a leaf, then return V as follows:

        V_1     where K < K_1
        V_2     where K_1 <= K < K_2
        ...
        V_(n)   where K_(n-1) <= K < K_n
        V_(n+1) where K_n <= K

        :param key: search key
        :return: value or (boolean, value)
        """
        # Getting values for leaves is different from internal nodes
        if self.leaf:
            return self.get_leaf_value(key)

        # Before the first key
        if key < self.keys[0]:
            return self.values[0]

        # In between one of the middle keys
        for i in range(self.size):
            if self.keys[i] <= key and (self.keys[i+1] is None or key < self.keys[i + 1]):
                return self.values[i + 1]

        # After the last key
        return self.values[:-1]

    def get_leaf_value(self, key):
        """
        The key should match a key in this Block. Return tuple with boolean True if key was matched in the Block
        :param key:
        :return: (FoundValue, value)
        """
        for i, k in enumerate(self.keys):
            if key == k:
                return True, self.values[i]
        return False, self.values[-1]

    def insert_single(self, key, left_value, right_value):
        """
        With an Empty Block, insert a single key with a right and left value. This method is to be used
        after a root split
        :param key: first key for splitting
        :param left_value: block to the left
        :param right_value: block to the right
        :return:
        """
        self.keys[0] = key
        self.values[0] = left_value
        self.values[1] = right_value

    def insert(self, key, value):
        """
        Inserts a key value pair into the block while maintaining the order of the keys and values.

        If there are now size + 1 keys, the block will be split such that the lower half of the keys and values
        will be stored in this block and the upper half of the keys and values are in a new block. The median key is
        returned as a tuple with the new block.

        :param key:
        :param value:
        :return: None or (key, block)
        """
        if any([k == key for k in self.keys]):
            raise Exception("Attempted to insert duplicate key [%s]" % key)

        # If there is space for another key
        if any([k is None for k in self.keys]):
            return self.insert_into_block(key, value)

        # Otherwise split the block based on sorted key values pairs
        keys = list(self.keys.tolist())
        values = list(self.values.tolist())
        if key < keys[0]:
            values.insert(0, value)
        else:
            for i, k in enumerate(keys):
                if key < k:
                    keys.insert(i, key)
                    values.insert(i + 1, value)
            if key > keys[-1]:
                keys.append(key)
                values.insert(-1, value)

        # Create a new Block
        new_block = Block(self.size, self.leaf)

        # Split the keys
        key_keep = int(math.ceil(self.size / 2))
        median_key = keys[key_keep]
        new_keys = keys[key_keep:]
        self.keys[key_keep:] = None

        # Split the values
        value_keep = int(math.ceil(len(values) / 2))
        new_values = values[value_keep-1:]
        self.values[value_keep:] = None
        self.values[-1] = new_block

        # Update the new Block
        new_block.set_after_split(new_keys, new_values)

        return median_key, new_block

    def insert_into_block(self, key, value):
        for i in range(self.size):
            if self.keys[i] is None:
                self.keys[i] = key
                self.values[i] = value
                return None
            elif key < self.keys[i]:
                # Shift the keys and values
                self.keys[i + 1:] = self.keys[i:-1]
                self.values[i + 1:-1] = self.values[i:-2]

                # Insert the key value pair
                self.keys[i] = key
                self.values[i] = value

                # Return without work to do
                return None
        raise Exception("This should be unreachable. Our key sort integrity has been violated")

    def set_after_split(self, keys, values):
        """
        Setter for private use when inserting into the BTree and splitting when there are too many keys
        :param keys: already sorted
        :param values: values for the keys of a leaf block
        """
        # Copy the keys
        self.keys[:len(keys)] = keys

        # Copy the values
        self.values[:len(values)] = values
