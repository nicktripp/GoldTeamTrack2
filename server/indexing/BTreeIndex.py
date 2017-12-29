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
                k = table.parse_value_for_column(k, column_name)
            except ValueError:
                print("Error parsing at location %s for %s in %s" % (v, k, column_name))
                assert False

            # Accumulate 3 unique keys for the initial pairs
            if k in initial_pairs:
                initial_pairs[k].add(v)
            else:
                initial_pairs[k] = {v}

        # Create a BTreeIndex with the pairs
        index = BTreeIndex(initial_pairs, sparse)

        # Insert the rest of the items in the generator
        try:
            for k, v, _ in pair_generator:
                try:
                    k = table.parse_value_for_column(k, column_name)
                except ValueError:
                    print("Error parsing at location %s for %s in %s" % (v, k, column_name))
                    assert False
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

    def size(self):
        return self.btree.n

    def index_to_index_op(self, other, operator, negated, left_transformer, right_transformer, left_index, right_index):
        if (operator == '=' and not negated) or (operator == '<>' and negated):
            yield from self.index_equals_index(other, left_transformer, right_transformer)
        else:
            # For each key values pair in right_index
            right_items = [(k, vs) for k, vs in other.items()]
            for k, vs in right_items:
                # Transform key if column was given math requirements ie S.a + 5
                k = right_transformer(k)

                # Get the row pairs that satisfy for each table's column
                left_rows = self.op(k, operator, negated)
                if left_index <= right_index:
                    for lr in left_rows:
                        for v in vs:
                            yield (lr, v)
                else:
                    for v in vs:
                        for lr in left_rows:
                            yield (v, lr)

    def index_equals_index(self, other, left_transformer, right_transformer):
        # Consider all matching keys in each index
        val1, block1 = self.btree.get_with_block(self.btree.smallest)
        val2, block2 = other.btree.get_with_block(other.btree.smallest)
        i1, i2 = 1, 1

        # Iterate through the keys of the smaller index, skipping through the keys of the larger
        smaller_index, larger_index = other, self
        k1 = left_transformer(larger_index.btree.smallest)
        k2 = right_transformer(smaller_index.btree.smallest)
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
                i, j = i1 - 1, i2 - 1
                for v1 in block1.values[i]:
                    for v2 in block2.values[j]:
                        yield (v1, v2)

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

    def op(self, key, comparison, negated=False):
        """
        The key will be compared against all of the keys in the index with the provided comparison

        <, >, <>, =, etc.
        :param key:
        :param comparison:
        :return: set of row locations in file
        """
        if (comparison == '=' and not negated) or (comparison == '<>' and negated):
            yield from self.equal(key)
        elif (comparison == '<' and not negated) or (comparison == '>=' and negated):
            yield from self.lessThan(key)
        elif (comparison == '<=' and not negated) or (comparison == '>' and negated):
            yield from self.lessThanOrEqual(key)
        elif (comparison == '<>' and not negated) or (comparison == '=' and negated):
            yield from self.notEqual(key)
        elif (comparison == '>' and not negated) or (comparison == '<=' and negated):
            yield from self.greaterThan(key)
        elif (comparison == '>=' and not negated) or (comparison == '<' and negated):
            yield from self.greaterThanOrEqual(key)
        elif comparison == 'LIKE':
            yield from self.like(key, negated)

    def equal(self, key):
        values = self.btree[key]
        if values is not None:
            for v in values:
                yield v

    def notEqual(self, key):
        val, key_block = self.btree.get_with_block(key)
        val, block = self.btree.get_with_block(self.btree.smallest)
        while block != key_block:
            for i in range(len(block.keys)):
                for v in block.values[i]:
                    yield v
            block = block.next_leaf

        for i in range(len(block.keys)):
            if key != block.keys[i]:
                for v in block.values[i]:
                    yield v
        block = block.next_leaf

        while block is not None:
            for i in range(len(block.keys)):
                for v in block.values[i]:
                    yield v
            block = block.next_leaf

    def lessThan(self, key):
        val, stop_block = self.btree.get_with_block(key)
        val, block = self.btree.get_with_block(self.btree.smallest)
        # Everything is less than until we get to the block that holds the matching key
        while stop_block != block:
            for i in range(len(block.keys)):
                for v in block.values[i]:
                    yield v
            block = block.next_leaf

        # This is the only block that does a comparison
        for i in range(len(block.keys)):
            if key <= stop_block.keys[i]:
                break
            for v in block.values[i]:
                yield v

    def lessThanOrEqual(self, key):
        val, stop_block = self.btree.get_with_block(key)
        val, block = self.btree.get_with_block(self.btree.smallest)
        # Everything is less than until we get to the block that holds the matching key
        while stop_block != block:
            for i in range(len(block.keys)):
                for v in block.values[i]:
                    yield v
            block = block.next_leaf

        # This is the only block that does a comparison
        for i in range(len(block.keys)):
            if key < stop_block.keys[i]:
                break
            for v in block.values[i]:
                yield v

    def greaterThan(self, key):
        val, block = self.btree.get_with_block(key)
        while True:
            for i in range(len(block.keys)):
                if block.keys[i] > key:
                    for v in block.values[i]:
                        yield v
            if block.next_leaf is None:
                break
            block = block.next_leaf

    def greaterThanOrEqual(self, key):
        val, block = self.btree.get_with_block(key)
        while True:
            for i in range(len(block.keys)):
                if block.keys[i] >= key:
                    for v in block.values[i]:
                        yield v
            if block.next_leaf is None:
                break
            block = block.next_leaf

    def like(self, key, negated):
        val, block = self.btree.get_with_block(self.btree.smallest)
        pattern = re.compile(key.replace("%", ".*"))
        while True:
            for i in range(len(block.keys)):
                check = pattern.match(block.keys[i])
                if (check and not negated) or (not check and negated):
                    for v in block.values[i]:
                        yield v
            if block.next_leaf is None:
                break
            block = block.next_leaf
