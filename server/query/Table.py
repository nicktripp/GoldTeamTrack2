import os


class Table:

    relative_path = "./data/%s.csv"

    def __init__(self, name, nickname=None):
        self._name = name
        self._nickname = nickname
        assert os.path.exists(self.filename), "The table must exist."

        # Fill the column index
        with open(self.filename, 'r') as f:
            columns = f.readline()[:-1].split(',')
            self._column_index = {col: i for i, col in enumerate(columns)}

    def __repr__(self):
        return "Table " + self.name

    def __eq__(self, other):
        if isinstance(other, Table):
            if self.nickname is not None and self.nickname != other.nickname:
                return False
            return self.name == other.name
        return False

    @property
    def name(self):
        return self._name

    @property
    def column_index(self):
        return self._column_index

    @property
    def nickname(self):
        return self._nickname

    @property
    def filename(self):
        return Table.relative_path % self.name
