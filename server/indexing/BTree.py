import numpy as np
import math

from server.indexing.Block import Block


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

    def __repr__(self):
        return str(self.root)

    def __init__(self, blocksize, initial_values):
        """
        Creates a B+ Tree with Blocks of with blocksize keys and blocksize + 1 values per block.
        initial_values is required to have more than blocksize key-value pairs

        :param blocksize: used to set the number of keys per block
        :param initial_values: dict of initial values to be inserted into
        """
        unique_init = len(initial_values.keys())
        if unique_init < blocksize:
            # We need the root to split during the initialization
            unique_init -= 1
            if unique_init > 3:
                blocksize = unique_init - (unique_init % 2) # only use even block sizes
                print("WARNING: Using block size of %d" % blocksize)
            else:
                raise Exception("There are not enough unique column values to index")

        # Create the first block
        self.blocksize = blocksize
        self.root = Block(blocksize, True)

        # Insert initial values so that root is an internal node
        for key in initial_values:
            self.insert(key, initial_values[key])

    def lookup(self, key):
        """
        Assume there are no duplicate keys at the leaves and every search-key value that appears in the datafile
        will also appear at a leaf.
        :return:
        """
        return self.lookup_recurse(key, self.root)

    def lookup_recurse(self, key, block):
        if block.leaf:
            return block.get_value(key)

        return self.lookup_recurse(key, block.get_value(key))

    def insert(self, key, value):
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
        """
        root_keys = sum(k is not None for k in self.root.keys)
        root_insert = self.insert_off_root(key, value, self.root)

        if root_insert is None:
            return None

        root_key, right = root_insert
        # If root split reassign root
        if sum(k is not None for k in self.root.keys) < root_keys:
            left = self.root
            self.root = Block(self.blocksize, False)
            self.root.insert_single(root_key, left, right)
        # Otherwise add the new root key and child
        else:
            for i in range(self.blocksize):
                if self.root.keys[i] is None:
                    self.root.keys[i] = root_key
                    self.root.values[i+1] = right
                    break
                elif root_key < self.root.keys[i]:
                    # Shift the keys and values
                    self.root.keys[i+1:] = self.root.keys[i:-1]
                    self.root.values[i+1:] = self.root.values[i:-1]

                    # Insert the new key value pair
                    self.root.keys[i] = root_key
                    self.root.values[i+1] = right
                    break

    def insert_off_root(self, key, value, block):
        if any([k == key for k in block.keys]):
            raise Exception("Attempted to insert duplicate key [%s]" % key)

        # Handle leaf case
        if block.leaf:
            return block.insert(key, value)

        # Check if key is less than all keys in current block
        if key < block.keys[0]:
            return block.values[0].insert(key, value)

        # Check if key in between keys of block
        for i in range(1, self.blocksize):
            if block.keys[i - 1] <= key and (block.keys[i] is None or key < block.keys[i]):
                return block.values[i].insert(key, value)

        # Check if key is greater than all keys in current block
        if key >= block.keys[-1]:
            return block.values[-1].insert(key, value)
