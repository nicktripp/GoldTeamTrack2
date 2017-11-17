import numpy as np
import math


class Block:
    """
    BTree Block
    """

    # Determines the number of keys in all blocks
    size = 4

    def __init__(self, is_leaf=True):
        """
        Initializes a Block
        The Block is assumed to be a Leaf unless told otherwise
        """
        self.leaf = is_leaf
        self.keys = np.array([None] * Block.size)
        self.values = np.array([None] * (Block.size + 1))

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
            for i, k in enumerate(self.keys):
                if key == k:
                    return True, self.values[i]
            return False, self.values[-1]

        # Before the first key
        if key < self.keys[0]:
            return self.values[0]

        # In between one of the middle keys
        for i in range(Block.size):
            if self.keys[i] <= key < self.keys[i + 1]:
                return self.values[i + 1]

        # After the last key
        return self.values[:-1]

    def insert_single(self, key, left_value, right_value):
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
        # If there is space for another key
        if any([k is None for k in self.keys]):
            for i in range(Block.size):
                if key < self.keys[i]:
                    # Shift the keys and values
                    self.keys[i + 1:] = self.keys[i:-1]
                    self.values[i + 1:-1] = self.values[i:-2]

                    # Insert the key value pair
                    self.keys[i] = key
                    self.values[i] = value

                    # Return without work to do
                    return None
            raise Exception("This should be unreachable. Our key sort integrity has been violated")

        # The keys and values must be split
        keys_leaving = math.floor(Block.size / 2)
        values_leaving = math.floor((Block.size + 2) / 2)
        new_block = Block(self.leaf)
        new_block.set_after_split(self.keys[:-keys_leaving], self.values[:-values_leaving])

        # Clear space in this Block
        self.keys[:-keys_leaving] = None
        self.values[:-values_leaving] = None

        # Add next block pointer
        self.values[-1] = new_block

        # self is not a leaf Block
        self.leaf = False

        # Return a tuple to be inserted above
        return self.keys[math.ceil(Block.size / 2) + 1], new_block

    def set_after_split(self, keys, values):
        """
        Setter for private use when inserting into the BTree and splitting when there are too many keys
        :param keys: already sorted
        :param values: values for the keys of a leaf block
        """
        # Copy the keys
        self.keys[:len(keys)] = keys

        # Copy the values, mindful of the final block pointer
        self.values[:len(values) - 1] = values[:-1]
        self.values[-1] = values[-1]


class BTree:
    """
    B+ Tree implementation from textbook

    Characteristics:
    * The key in leaf nodes are copies of keys from the data file.
        These keys are distributed among the leaves in sorted order, from left to right.

    * At the root, there are at least two used pointers.
        All pointers point to Btree blocks at the level below.

    * At a leaf, the last pointer points to the next leaf block to the right,
        which is the block with the next higher keys. Among the other n pointers in a leaf block, at least (n + 1) // 2
        of these pointers are used and point to data records; unused pointers are null and do not point anywhere.
        The ith pointer, if it is used, points to a record with the ith key.

    * At an interior node, all n + 1 pointers can be used to point to Btree blocks at the next lower level. At least
        (n + 1) / 2 (ceil) of them are actually used ( but if the node is the root, then we require only that at
        least 2 be used, regardless of how large n is). If j pointers are used, then there will be j - 1 keys,
        say K_1, K_2, ..., K_(j-1). The first pointer points to a part of the Btree where some of the records with keys
        less than K_1 will be found. The second pointer goes to that part of the tree where all records with keys
        that are at least K_1, but less than K_2 will be found, and so on. Finally, the jth pointer gets us to the part
        of the Btree where some of the records with keys greater than or equal to K_(j-1) are found.
        Note that some records with keys far below K_1 or far above K_(j-1) may not be reachable from this block at all,
        but will be reached via another block at the same level.

    * All used pointers and their keys appear at the beginning of the block with the exception of the (n+1)st pointer
        in a leaf, which points to the next leaf.
    """

    def __init__(self, blocksize):
        """
        Creates a B+ Tree with blocksize keys per block.
        :param blocksize:
        """
        print("Creating B+ Tree")
        self.root = Block()

    def lookup(self, key, block):
        """
        Assume there are no duplicate keys at the leaves and every search-key value that appears in the datafile
        will also appear at a leaf.

        :return:
        """

        # If we are at a leaf, look among the keys there.
        if block.is_leaf():
            return block.get_value(key)

        # We are at an interior node
        next = block.next_block(key)
        return self.lookup(key, next)

    def insert(self, key, value, block, root=False):
        """
        * We try to find a place for the new key in the appropriate leaf, and we put it there if there is room.

        * If there is no room in the proper leaf, we split the leaf into two and divide the keys between the two new
            nodes, so each is half full or just over half full.

        * The splitting of nodes at one level appears to the level above as if a new key-pointer pair needs to be
            inserted at that higher level. We may thus recursively apply this strategy to insert at the next level:
            if there is room, insert it; if not, split the parent node and continue up the tree.

        * As an exception, if we try to insert into the root, and there is no room, then we split the root into two
            nodes and create a new root at the next higher level; the new root has the two nodes resulting from the
            split as its children. Recall that no matter how large n (the number of slots for the keys at a node) is,
            it is always permissible for the root to have only one key and two children.
        :param key: key to insert
        :param value: value associated with key
        :return: None or (key, value) to insert
        """
        if block.is_leaf():
            return block.insert(key, value)

        # As an internal node, insert the block into the appropriate child block
        insert_result = None

        # Check if key is less than all keys in current block
        if key < block.keys[0]:
            insert_result = block.insert(key, value, block.values[0])

        # Check if key in between keys of block
        for i in range(1, Block.size):
            if block.keys[i - 1] <= key < block.keys[i]:
                insert_result = block.insert(key, value, block.values[i])
                break

        # Check if key is greater than all keys in current block
        if key >= block.keys[-1]:
            insert_result = block.insert(key, value, block.values[-1])

        # Handle the output of the insertion, including child splits
        if insert_result is None:
            return None

        # Root must handle splits, internal nodes can pass them up
        new_key = insert_result[0]
        new_block = insert_result[1]
        if not root:
            return block.insert(new_key, new_block)
        else:
            root_insert = block.insert(new_key, new_block)
            # Successfully insert to root
            if root_insert is None:
                return None
            # Split root
            else:
                # Root was split, create Block with single key and a left child and a right child
                root_key, right = root_insert
                left = self.root
                self.root = Block()
                self.root.insert_single(root_key, left, right)
                return None
