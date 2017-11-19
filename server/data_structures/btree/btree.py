class BTree:
    """
    B+ Tree implementation
    """
    def __init__(self, block_size, initial_values):
        self.block_size = block_size
        self.root = None

    def __repr__(self):
        return self.root

    def __getitem__(self, key):
        """
        Support key lookup with brackets
        """
        return None

    def __setitem__(self, key, value):
        """
        Support key-value insertion with brackets
        """
        return value
