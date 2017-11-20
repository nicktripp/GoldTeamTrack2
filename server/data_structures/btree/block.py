from abc import abstractmethod


class Block:
    """
    Block is a Node in a B+ Tree with keys and values
    keys are stored in ascending order
    """
    def __init__(self, size):
        """
        Initialize a Block as a node in a B+ Tree, deferring
        insertion and lookup logic to sub classes
        """
        # Number of keys
        self.size = size

        # Ordered key value pairs
        self.keys = []
        self.values = []

    @abstractmethod
    def __getitem__(self, key):
        raise Exception("Not implemented for super class Block")

    def insert(self, key, value):
        """
        Inserts key into a leaf block. This may cause the leaf to split.
        :param key:
        :param value:
        :return: split_result
        """
        if key in self:
            raise Exception("Cannot insert duplicate keys :: %s" % ((key, value),))

        if self.is_full():
            return self.insert_with_split(key, value)
        else:
            return self.insert_without_split(key, value)

    @abstractmethod
    def insert_with_split(self, key, value):
        raise Exception("Not implemented for super class Block")

    @abstractmethod
    def insert_without_split(self, key, value):
        raise Exception("Not implemented for super class Block")

    def get_key_index(self, key):
        """
        Returns the index of a key in the block's keys
        If the key is not found, then -1 is returned
        :param key:
        :return: index
        """
        for i, ki in enumerate(self.keys):
            if ki == key:
                return i
        return -1

    def __contains__(self, key):
        """
        return True if the key is in block's keys
        :param key:
        :return:
        """
        return self.get_key_index(key) != -1

    def is_full(self):
        return len(self.keys) >= self.size
