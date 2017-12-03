from server.query.Table import Table


class Column:

    def __init__(self, table, name):
        assert isinstance(table, Table), "A Column must belong to a Table."
        self._name = name
        self._table = table

    def __eq__(self, other):
        if isinstance(other, Column):
            return self.table == other.table and self.name == other.name
        return False

    @property
    def name(self):
        return self._name

    @property
    def table(self):
        return self._table