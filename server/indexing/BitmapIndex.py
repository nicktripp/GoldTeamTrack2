from server.data_structures.bitmap.Bitmap_Entry import Bitmap_Entry
from server.data_structures.bitmap.bitmap import Bitmap
from server.data_structures.btree.btree import BTree
from server.indexing import TableIndexer
from server.indexing.BTreeIndex import BTreeIndex
from server.query.Table import Table

import re

class BitmapIndex:

    def __init__(self, initial_bitstring_pairs, initial_record_pairs, block_size = 4):

        self.bitstringTree = BTree(block_size, initial_bitstring_pairs)
        self.recordsTree = BTree(block_size, initial_record_pairs)

    @staticmethod
    def make(pair_generator):

        initial_bitstring_pairs = {}
        initial_record_pairs = {}

        while(len(initial_bitstring_pairs) < 3):
            try:
                key, mem_location, record_num = next(pair_generator)
            except StopIteration:
                assert False, "There are not enough unique values to index this row."

            k = Table.parse_value(key)

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

