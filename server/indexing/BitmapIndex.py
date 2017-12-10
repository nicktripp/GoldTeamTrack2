from server.data_structures.bitmap.Bitmap_Entry import Bitmap_Entry
from server.data_structures.bitmap.bitmap import Bitmap
from server.data_structures.btree.btree import BTree
from server.indexing import TableIndexer
from server.indexing.BTreeIndex import BTreeIndex


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
            k = TableIndexer.TableIndexer.parse_value(key)

            new_bitstring_index = Bitmap_Entry(key,record_num)

            if k in initial_bitstring_pairs:
                initial_bitstring_pairs[k].append(record_num)
            else:
                initial_bitstring_pairs[k] = new_bitstring_index

            assert k not in initial_record_pairs, "No duplicate record numbers!"
            initial_record_pairs[k] = mem_location


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

                assert record_num not in index.recordsTree
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
        values = self.bitstringTree[key].populate_record_list()
        # List of memory locations
        ret = []
        for value in values:
            ret.append(self.recordsTree[value])

        return ret

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
            if key <= stop_block.keys[i]:
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
            if key < stop_block.keys[i]:
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
