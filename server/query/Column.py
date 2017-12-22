
from server.query.Table import Table


class Column:

    def __init__(self, table, name, op=None, number=None):
        assert isinstance(table, Table), "A Column must belong to a Table."
        self._name = name
        self._table = table
        if op is not None:
            op = op.value
        self.op = op
        if number is not None:
            number = number.value
        self.number = number

    def __eq__(self, other):
        if isinstance(other, Column):
            return self.table == other.table and self.name == other.name
        return False

    def __hash__(self):
        return hash(repr(self))

    def __repr__(self):
        if self._table.nickname is None:
            return self._table.name + "." + self._name
        else:
            return self._table.nickname + "." + self._name

    def transform(self, key):
        if self.op is not None:
            if self.op == '*':
                return key * float(self.number)
            elif self.op == '/':
                return key / float(self.number)
            elif self.op == '+':
                return key + float(self.number)
            else:
                return key - float(self.number)
        else:
            return key

    def invert_transform(self, key):
        if self.op is not None:
            if self.op == '*':
                return key / float(self.number)
            elif self.op == '/':
                return key * float(self.number)
            elif self.op == '+':
                return key - float(self.number)
            else:
                return key + float(self.number)
        else:
            return key

    @property
    def name(self):
        return self._name

    @property
    def table(self):
        return self._table