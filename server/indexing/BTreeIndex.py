from server.data_structures.btree.btree import BTree
import pickle
import os
import time
import sys
import re

sys.setrecursionlimit(10000)


class BTreeIndex:

    def __init__(self, initial_pairs, block_size=10):
        # Initialize the BTree index with 3 unique key-value pairs
        self.btree = BTree(block_size, initial_pairs)

    @staticmethod
    def make(pair_generator):
        initial_pairs = {}

        # Get 3 initial values
        while len(initial_pairs) < 3:
            try:
                k, v = next(pair_generator)
            except StopIteration:
                assert False, "There are not enough unique values to index this row."
            if k in initial_pairs:
                initial_pairs[k].add(v)
            else:
                initial_pairs[k] = {v}

        # Create a BTreeIndex with the pairs
        index = BTreeIndex(initial_pairs)

        # Insert the rest of the items in the generator
        try:
            for k, v in pair_generator:
                lookup = index.btree[k]
                if lookup:
                    lookup.add(v)
                else:
                    index.btree[k] = {v}
        except StopIteration:
            # There were exactly 3 unique values
            # All of the rows were consumed before we got here
            pass

        # Return the filled index
        return index

    def items(self):
        return self.btree.items()

    def op(self, key, comparison):
        """
        The key will be compared against all of the keys in the index with the provided comparison

        <, >, <>, =, etc.
        :param key:
        :param comparison:
        :return: set of row locations in file
        """
        if comparison == '=':
            return self.equal(key)
        elif comparison == '<':
            return self.lessThan(key)
        elif comparison == '<=':
            return self.lessThanOrEqual(key)
        elif comparison == '<>':
            return self.notEqual(key)
        elif comparison == '>':
            return self.greaterThan(key)
        elif comparison == '>=':
            return self.greaterThanOrEqual(key)
        elif comparison == 'LIKE':
            return self.like(key)
        return None

    def equal(self, key):
        values = self.btree[key]
        return {key: values}

    def notEqual(self, key):
        val, key_block = self.btree.get_with_block(key)
        val, block = self.btree.get_with_block(self.btree.smallest)
        ret = {}
        while block != key_block:
            for i in range(len(block.keys)):
                ret[block.keys[i]] = block.values[i]
            block = block.next_leaf

        for i in range(len(block.keys)):
            if key != block.keys[i]:
                ret[block.keys[i]] = block.values[i]
        block = block.next_leaf

        while block is not None:
            for i in range(len(block.keys)):
                ret[block.keys[i]] = block.values[i]
            block = block.next_leaf
        return ret

    def lessThan(self, key):
        val, stop_block = self.btree.get_with_block(key)
        val, block = self.btree.get_with_block(self.btree.smallest)
        ret = {}
        # Everything is less than until we get to the block that holds the matching key
        while stop_block != block:
            for i in range(len(block.keys)):
                ret[block.keys[i]] = block.values[i]
            block = block.next_leaf

        # This is the only block that does a comparison
        for i in range(len(block.keys)):
            if key >= stop_block.keys[i]:
                break
            ret[block.keys[i]] = block.values[i]
        return ret

    def lessThanOrEqual(self, key):
        val, stop_block = self.btree.get_with_block(key)
        val, block = self.btree.get_with_block(self.btree.smallest)
        ret = {}
        # Everything is less than until we get to the block that holds the matching key
        while stop_block != block:
            for i in range(len(block.keys)):
                ret[block.keys[i]] = block.values[i]
            block = block.next_leaf

        # This is the only block that does a comparison
        for i in range(len(block.keys)):
            if key > stop_block.keys[i]:
                break
            ret[block.keys[i]] = block.values[i]
        return ret

    def greaterThan(self, key):
        val, block = self.btree.get_with_block(key)
        ret = {}
        while True:
            for i in range(len(block.keys)):
                if block.keys[i] > key:
                    ret[block.keys[i]] = block.values[i]
            if block.next_leaf is None:
                return ret
            block = block.next_leaf

    def greaterThanOrEqual(self, key):
        val, block = self.btree.get_with_block(key)
        ret = {}
        while True:
            for i in range(len(block.keys)):
                if block.keys[i] >= key:
                    ret[block.keys[i]] = block.values[i]
            if block.next_leaf is None:
                return ret
            block = block.next_leaf

    def like(self, key):
        val, block = self.btree.get_with_block(self.btree.smallest)
        ret = {}
        while True:
            for i in range(len(block.keys)):
                pattern = re.compile(key.replace("%", ".*"))
                check = pattern.match(block.keys[i])
                if check:
                    ret[block.keys[i]] = block.values[i]
            if block.next_leaf is None:
                return ret
            block = block.next_leaf
