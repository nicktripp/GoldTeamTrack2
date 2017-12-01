from server.query.Table import Table


class Column:

    def __init__(self, table, name):
        assert isinstance(table, Table), "A Column must belong to a Table."
        self._name = name

    @property
    def name(self):
        return self._name