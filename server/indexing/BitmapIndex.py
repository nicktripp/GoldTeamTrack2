from server.data_structures.bitmap.Bitmap_Entry import Bitmap_Entry
from server.data_structures.bitmap.bitmap import Bitmap
from server.data_structures.btree.btree import BTree
from server.indexing import TableIndexer
from server.indexing.BTreeIndex import BTreeIndex
from server.query.Table import Table

import re

from server.query import Table


class BitmapIndex:

    def __init__(self, initial_bitstring_pairs, initial_record_pairs, block_size = 4):

        self.bitstringTree = BTree(block_size, initial_bitstring_pairs, False)
        self.recordsTree = BTree(block_size, initial_record_pairs, False)

    @staticmethod
    def make(pair_generator, table, column_name):

        initial_bitstring_pairs = {}
        initial_record_pairs = {}

        while(len(initial_bitstring_pairs) < 3):
            try:
                key, mem_location, record_num = next(pair_generator)
            except StopIteration:
                assert False, "There are not enough unique values to index this row."

            k = table.parse_value(key)

            new_bitstring_index = Bitmap_Entry(key,record_num)

            if k in initial_bitstring_pairs:
                initial_bitstring_pairs[k].append(record_num)
            else:
                initial_bitstring_pairs[k] = new_bitstring_index

            assert k not in initial_record_pairs, "No duplicate record numbers!"
            initial_record_pairs[record_num] = mem_location


        # Create a BTreeIndex with the pairs
        index = BitmapIndex(initial_bitstring_pairs, initial_record_pairs)

        # Insert the rest of the items in the generator
        try:
            for key, mem_location, record_num in pair_generator:
                k = TableIndexer.TableIndexer.parse_value(key)

                lookup = index.bitstringTree[k]
                if lookup:
                    lookup.append(record_num)
                else:
                    new_index = Bitmap_Entry(k, record_num)
                    index.bitstringTree[k] = new_index

                assert record_num not in index.recordsTree, "Duplicate Record Numbers Found!"
                index.recordsTree[record_num] = mem_location

        except StopIteration:
            # There were exactly 3 unique values
            # All of the rows were consumed before we got here
            pass

        # Return the filled index
        for k,bitmap_entry in index.bitstringTree.items():
            bitmap_entry.encode_compressed_bitstring();

        return index

    def index_equals_index(self, other, left_transformer, right_transformer):
        # Consider all matching keys in each index
        val1, block1 = self.bitstringTree.get_with_block(self.btree.smallest)
        val2, block2 = other.bitstringTree.get_with_block(other.btree.smallest)
        i1, i2 = 1, 1

        # Iterate through the keys of the smaller index, skipping through the keys of the larger
        smaller_index, larger_index = other, self
        k1 = left_transformer(larger_index.bitstringTree.smallest)
        k2 = right_transformer(smaller_index.bitstringTree.smallest)
        while block1 is not None and block2 is not None:
            if k1 < k2:
                # If the larger index has a smaller key, move the key forward until they match
                while block1 is not None and k1 < k2:
                    if i1 < len(block1.keys):
                        # Get the next key in the block
                        k1 = left_transformer(block1.keys[i1])
                        i1 += 1
                    else:
                        # Get the next block
                        block1 = block1.next_leaf
                        i1 = 0
            elif k1 > k2:
                # If the smaller index has a smaller key, move the key forward until they match
                while block2 is not None and k2 < k1:
                    if i2 < len(block2.keys):
                        # Get the next key in the block
                        k2 = right_transformer(block2.keys[i2])
                        i2 += 1
                    else:
                        # Get the next block
                        block2 = block2.next_leaf
                        i2 = 0

            # If the keys are equal, yield the cartesian product of their values
            if k1 == k2:
                bentry1 = block1.values[k1].populate_record_list()
                bentry2 = block2.values[k2].populate_record_list()
                for v1 in bentry1:
                    mem1 = larger_index.recordsTree[v1]
                    for v2 in bentry2:
                        mem2 = smaller_index.recordsTree[v2]
                        yield (mem1, mem2)

            # Stop if there are no more keys to consider
            if block2 is None:
                break

            # Move the smaller index to the next key
            if i2 < len(block2.keys):
                # Get the next key in the block
                k2 = right_transformer(block2.keys[i2])
                i2 += 1
            else:
                # Get the next block
                block2 = block2.next_leaf

                # Get the next key
                if block2 is None:
                    break
                k2 = right_transformer(block2.keys[0])
                i2 = 1

    def __repr__(self):
        return str(self.bitstringTree)


    def op(self, key, comparison, negated=False):
        """
        The key will be compared against all of the keys in the index with the provided comparison

        <, >, <>, =, etc.
        :param key:
        :param comparison:
        :return: set of row locations in file
        """


        if (comparison == '=' and not negated) or (comparison == '<>' and negated):
            return self.equal(key)
        elif (comparison == '<' and not negated) or (comparison == '>=' and negated):
            return self.lessThan(key)
        elif (comparison == '<=' and not negated) or (comparison == '>' and negated):
            return self.lessThanOrEqual(key)
        elif (comparison == '<>' and not negated) or (comparison == '=' and negated):
            return self.notEqual(key)
        elif (comparison == '>' and not negated) or (comparison == '<=' and negated):
            return self.greaterThan(key)
        elif (comparison == '>=' and not negated) or (comparison == '<' and negated):
            return self.greaterThanOrEqual(key)
        elif comparison == 'LIKE':
            return self.like(key, negated)
        return None

    def equal(self, key):
        values = self.bitstringTree[key].populate_record_list()
        # List of memory locations
        ret = []
        for value in values:
            ret.append(self.recordsTree[value])

        return ret

    def notEqual(self, key):

        values = set(self.bitstringTree[key].populate_record_list())
        ret = []

        for i in range(self.recordsTree.n):
            if i not in values:
                ret.append(self.recordsTree[i])

        return ret

    def lessThan(self, key):
        val, stop_block = self.bitstringTree.get_with_block(key)
        val, block = self.bitstringTree.get_with_block(self.bitstringTree.smallest)
        ret = []

        # Everything is less than until we get to the block that holds the matching key
        agg_bitstring = "0" * self.recordsTree.n

        while stop_block != block:
            for i in range(len(block.keys)):
                new_bs = block.values[i].decode_compressed_string(self.recordsTree.n)
                agg_bitstring = self.do_or(agg_bitstring, new_bs)

            block = block.next_leaf

        if stop_block == block:
            for i in range(len(block.keys)):
                if(block.keys[i] < key):
                    new_bs = block.values[i].decode_compressed_string(self.recordsTree.n)
                    agg_bitstring = self.do_or(agg_bitstring, new_bs)

        # Iterate through the bitstring
        for i in range(len(agg_bitstring)):
            if(agg_bitstring[i] == '1'):
                ret.append(self.recordsTree[i])

        return ret


    def lessThanOrEqual(self, key):
        val, stop_block = self.bitstringTree.get_with_block(key)
        val, block = self.bitstringTree.get_with_block(self.bitstringTree.smallest)

        ret = []
        # Everything is less than until we get to the block that holds the matching key
        agg_bitstring = "0" * self.recordsTree.n

        while stop_block != block:
            for i in range(len(block.keys)):
                new_bs = block.values[i].decode_compressed_string(self.recordsTree.n)
                agg_bitstring = self.do_or(agg_bitstring, new_bs)
            block = block.next_leaf

        if stop_block == block:
            for i in range(len(block.keys)):
                if(block.keys[i] <= key):
                    new_bs = block.values[i].decode_compressed_string(self.recordsTree.n)
                    agg_bitstring = self.do_or(agg_bitstring, new_bs)

        # Iterate through the bitstring
        for i in range(len(agg_bitstring)):
            if (agg_bitstring[i] == '1'):
                ret.append(self.recordsTree[i])
        return ret

    def greaterThan(self, key):
        val, block = self.bitstringTree.get_with_block(key)
        ret = []

        # Everything is less than until we get to the block that holds the matching key
        agg_bitstring = "0" * self.recordsTree.n

        # block != null maybe?
        while True:
            for i in range(len(block.keys)):
                if block.keys[i] > key:
                    new_bs = block.values[i].decode_compressed_string(self.recordsTree.n)
                    agg_bitstring = self.do_or(agg_bitstring, new_bs)
            if block.next_leaf is None:
                break
            block = block.next_leaf

        # Iterate through the bitstring
        for i in range(len(agg_bitstring)):
            if (agg_bitstring[i] == '1'):
                ret.append(self.recordsTree[i])

        return ret


    def greaterThanOrEqual(self, key):
        val, block = self.bitstringTree.get_with_block(key)
        ret = []
        agg_bitstring = "0" * self.recordsTree.n

        while True:
            for i in range(len(block.keys)):
                if block.keys[i] >= key:
                    new_bs = block.values[i].decode_compressed_string(self.recordsTree.n)
                    agg_bitstring = self.do_or(agg_bitstring, new_bs)

            if block.next_leaf is None:
                break
            block = block.next_leaf

        # Iterate through the bitstring
        for i in range(len(agg_bitstring)):
            if (agg_bitstring[i] == '1'):
                ret.append(self.recordsTree[i])

        return ret

    def like(self, key):
        val, block = self.bitstringTree.get_with_block(self.bitstringTree.smallest)
        ret = []
        agg_bitstring = "0" * self.recordsTree.n
        pattern = re.compile(key.replace("%", ".*"))

        while True:
            for i in range(len(block.keys)):
                check = pattern.match(block.keys[i])
                if check is not None:
                    new_bs = block.values[i].decode_compressed_string(self.recordsTree.n)
                    agg_bitstring = self.do_or(agg_bitstring, new_bs)

            if block.next_leaf is None:
                break
            block = block.next_leaf

        # Iterate through the bitstring
        for i in range(len(agg_bitstring)):
            if (agg_bitstring[i] == '1'):
                ret.append(self.recordsTree[i])

        return ret


    def do_and(self, bitstring_1, bitstring_2):
        zipped = zip(bitstring_1, bitstring_2)
        new_string = ""
        for one, two in zipped:
            new_string += str(int(one) & int(two))

        return new_string

    def do_or(self, bitstring_1, bitstring_2):
        zipped = zip(bitstring_1, bitstring_2)
        new_string = ""
        for one, two in zipped:
            new_string += (str(int(one) | int(two)))

        return new_string

