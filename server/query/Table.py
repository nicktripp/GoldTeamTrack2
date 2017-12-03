import os


class Table:
    def __init__(self, name, nickname=None):
        self._name = name
        self._nickname = nickname
        assert os.path.exists(self._name), "The table must exist."

        # Fill the column index
        with open(name, 'r') as f:
            columns = f.readline()[:-1].split(',')
            self._column_index = {col: i for i, col in enumerate(columns)}

    @property
    def name(self):
        return self._name

    @property
    def column_index(self):
        return self._column_index

    @property
    def nickname(self):
        return self._nickname
