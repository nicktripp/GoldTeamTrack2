from server.data_structures.btree.external_block import ExternalBlock
from server.data_structures.btree.internal_block import InternalBlock
from server.query.NoneVal import NoneVal


class BTree:
    """
    B+ Tree implementation
    """

    def __init__(self, block_size, initial_values, sparse):
        self.block_size = block_size
        self.sparse = sparse
        self._null_values = set()
        if not sparse:
            # Initialize BTree with a root and a left and right leaf
            self.root = InternalBlock(block_size)
            right = ExternalBlock(block_size)
            left = ExternalBlock(block_size, right)

            # Use 3 values to initialize the tree
            assert (len(initial_values) >= 3)
            sorted_keys = sorted([k for k in initial_values][:3])
            assert all(not isinstance(k, NoneVal) for k in sorted_keys)
            left.keys = [sorted_keys[0]]
            left.values = [initial_values[sorted_keys[0]]]
            right.keys = [sorted_keys[1], sorted_keys[2]]
            right.values = [initial_values[sorted_keys[1]], initial_values[sorted_keys[2]]]
            self.root.keys = [sorted_keys[1]]
            self.root.values = [left, right]
            self.smallest = sorted_keys[0]

            self.n = 3
        else:
            assert len(initial_values) == 2 or len(initial_values) == 1, "Sparse BTrees should have less than 1 or 2 " \
                                                                         "keys"
            # Allow boolean indexes
            self.root = ExternalBlock(block_size)

            # Put the initial values in a leaf node root
            assert all(not isinstance(k, NoneVal) for k in initial_values)
            self.root.keys = list(sorted(initial_values))
            self.root.values = list(initial_values[k] for k in self.root.keys)
            self.smallest = self.root.keys[0]
            self.n = len(initial_values)

    def __repr__(self):
        return str(self.root)

    def __contains__(self, item):
        return self.root[item] is not None

    def __getitem__(self, key):
        """
        Support key lookup with brackets
        """
        if isinstance(key, NoneVal):
            return self._null_values
        return self.root[key]

    def __setitem__(self, key, value):
        """
        Support key-value insertion with brackets
        """
        assert not self.sparse, "You may not insert into a sparse BTree"
        if isinstance(key, NoneVal):
            if isinstance(value, set):
                self._null_values |= value
            else:
                self._null_values.add(value)

        insert_result = self.root.insert_recurse(key, value)

        if insert_result[0]:
            key, right = insert_result[1], insert_result[2]
            root = InternalBlock(self.block_size)
            root.values = [self.root, right]
            root.keys = [key]
            self.root = root

        if key < self.smallest:
            self.smallest = key

        self.n += 1

    def get_with_block(self, key):
        return self.root.get_with_block(key)

    def items(self):
        # Get the smallest block
        block = self.get_with_block(self.smallest)[1]

        # Yield all of the key value pairs in an external block then lookup the next external block
        while True:
            for i in range(len(block.keys)):
                k, v = block.keys[i], block.values[i]
                yield k, v
            if block.next_leaf is None:
                break
            block = block.next_leaf

