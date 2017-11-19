from server.data_structures.btree.external_block import ExternalBlock
from server.data_structures.btree.internal_block import InternalBlock


class BTree:
    """
    B+ Tree implementation
    """
    def __init__(self, block_size, initial_values):
        # Initialize BTree with a root and a left and right leaf
        self.block_size = block_size
        self.root = InternalBlock(block_size)
        right = ExternalBlock(block_size)
        left = ExternalBlock(block_size, right)

        # Use 3 values to initialize the tree
        sorted_keys = sorted([k for k in initial_values][:3])
        left.keys = [sorted_keys[0]]
        left.values = [initial_values[sorted_keys[0]]]
        right.keys = [sorted_keys[1], sorted_keys[2]]
        right.values = [initial_values[sorted_keys[1]], initial_values[sorted_keys[2]]]
        self.root.keys = [sorted_keys[1]]
        self.root.values = [left, right]


    def __repr__(self):
        return str(self.root)

    def __getitem__(self, key):
        """
        Support key lookup with brackets
        """
        return self.root[key]

    def __setitem__(self, key, value):
        """
        Support key-value insertion with brackets
        """
        insert_result = self.root.insert_recurse(key, value)

        if insert_result is not None:
            key, right = insert_result
            root = InternalBlock(self.block_size)
            root.values = [self.root, right]
            root.keys = [key]
            self.root = root
