import re

from server.data_structures.btree.btree import BTree


class BTreeIndex:

    def __init__(self, initial_pairs, sparse, block_size=100):
        # Initialize the BTree index with 3 unique key-value pairs
        self.btree = BTree(block_size, initial_pairs, sparse)

    @staticmethod
    def make(pair_generator, table, column_name):
        print("Making index for %s.%s" % (table.name, column_name))

        initial_pairs = {}

        # Get 3 initial values
        sparse = False
        while len(initial_pairs) < 3:
            # Iterate through all key pairs until 3 unique values have been found
            try:
                k, v, _ = next(pair_generator)
            except StopIteration:
                sparse = True
                break

            # Parse the column value as one of the supported data types
            try:
                pk = table.parse_value_for_column(k, column_name)
            except ValueError:
                print("Error parsing at location %s for %s in %s" % (v, k, column_name))
                assert False

            # Accumulate 3 unique keys for the initial pairs
            if pk in initial_pairs:
                initial_pairs[pk].add(v)
            else:
                initial_pairs[pk] = {v}

        # Create a BTreeIndex with the pairs
        index = BTreeIndex(initial_pairs, sparse)

        # Insert the rest of the items in the generator
        try:
            for k, v, _ in pair_generator:
                try:
                    pk = table.parse_value_for_column(k, column_name)
                except ValueError:
                    print("Error parsing at location %s for %s in %s" % (v, k, column_name))
                    assert False
                lookup = index.btree[pk]
                if lookup:
                    lookup.add(v)
                else:
                    index.btree[pk] = {v}
        except StopIteration:
            # There were exactly 3 unique values
            # All of the rows were consumed before we got here
            pass

        # Return the filled index
        return index

    def items(self):
        return self.btree.items()

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
        values = self.btree[key]
        if values is not None:
            return list(values)
        else:
            return []

    def notEqual(self, key):
        val, key_block = self.btree.get_with_block(key)
        val, block = self.btree.get_with_block(self.btree.smallest)
        ret = []
        while block != key_block:
            for i in range(len(block.keys)):
                ret.extend(list(block.values[i]))
            block = block.next_leaf

        for i in range(len(block.keys)):
            if key != block.keys[i]:
                ret.extend(list(block.values[i]))
        block = block.next_leaf

        while block is not None:
            for i in range(len(block.keys)):
                ret.extend(list(block.values[i]))
            block = block.next_leaf
        return ret

    def lessThan(self, key):
        val, stop_block = self.btree.get_with_block(key)
        val, block = self.btree.get_with_block(self.btree.smallest)
        ret = []
        # Everything is less than until we get to the block that holds the matching key
        while stop_block != block:
            for i in range(len(block.keys)):
                ret.extend(list(block.values[i]))
            block = block.next_leaf

        # This is the only block that does a comparison
        for i in range(len(block.keys)):
            if key <= stop_block.keys[i]:
                break
            ret.extend(list(block.values[i]))
        return ret

    def lessThanOrEqual(self, key):
        val, stop_block = self.btree.get_with_block(key)
        val, block = self.btree.get_with_block(self.btree.smallest)
        ret = []
        # Everything is less than until we get to the block that holds the matching key
        while stop_block != block:
            for i in range(len(block.keys)):
                ret.extend(list(block.values[i]))
            block = block.next_leaf

        # This is the only block that does a comparison
        for i in range(len(block.keys)):
            if key < stop_block.keys[i]:
                break
            ret.extend(list(block.values[i]))
        return ret

    def greaterThan(self, key):
        val, block = self.btree.get_with_block(key)
        ret = []
        while True:
            for i in range(len(block.keys)):
                if block.keys[i] > key:
                    ret.extend(list(block.values[i]))
            if block.next_leaf is None:
                return ret
            block = block.next_leaf

    def greaterThanOrEqual(self, key):
        val, block = self.btree.get_with_block(key)
        ret = []
        while True:
            for i in range(len(block.keys)):
                if block.keys[i] >= key:
                    ret.extend(list(block.values[i]))
            if block.next_leaf is None:
                return ret
            block = block.next_leaf

    def like(self, key, negated):
        val, block = self.btree.get_with_block(self.btree.smallest)
        pattern = re.compile(key.replace("%", ".*"))
        ret = []
        while True:
            for i in range(len(block.keys)):
                check = pattern.match(block.keys[i])
                if (check and not negated) or (not check and negated):
                    ret.extend(list(block.values[i]))
            if block.next_leaf is None:
                return ret
            block = block.next_leaf
