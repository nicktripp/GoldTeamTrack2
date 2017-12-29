import sqlparse

from server.indexing.TableIndexer import TableIndexer
from server.query.Column import Column


class Comparison:
    """
    A Comparison instance has a right, a left, and an operator
    """

    def __init__(self, sqlparse_comparison):
        """
        Convert from a sqlparse Comparison
        :param sqlparse_comparison:
        """
        if sqlparse_comparison is None:
            return

        self._left = sqlparse_comparison.left
        self._right = sqlparse_comparison.right

        for token in sqlparse_comparison.tokens:
            if token.ttype == sqlparse.tokens.Comparison:
                self._operator = token.value

        assert self._operator is not None, "There must be a comparison operator"
        assert self._left is not None, "There must be a column on the left hand side of the comparison"
        assert self._right is not None, "There must be a column or constant on the right hand side of the comparison."

    def __repr__(self):
        return "%s %s %s" % (self._left, self._operator, self._right)

    @property
    def left_string(self):
        return self._left.value

    @property
    def right_string(self):
        return self._right.value

    def parse_op(self, operation):
        """
        Parses operation.

        NOTE: Assumes that the operation strictly follows format: <Column> <operation> <number>
        TODO: fix the above to support out of order and multiple col ops

        :param operation:
        :return: tuple containing: (tuple of (tablename, colname), operation, number)
        """
        real_name = operation.tokens[0].value   # TODO What if the operation is out-of-order? i.e. 2 * S.a
        l_op = operation.tokens[2]
        number = operation.tokens[4]            # TODO What if the operation uses two columns? i.e. S.a + S.b (not supported easily)
        names = real_name.split('.')
        return names, l_op, number

    def left_column(self, tables):
        """
        The comparison has not parsed the left and right to be actual columns yet

        Use the tables to find the correct table and column combination
        or raise and error
        :param tables: list of tables
        :return: Column for left hand side of the comparison
        """

        if type(self._left) == sqlparse.sql.Operation:
            names, l_op, number = self.parse_op(self._left)
        else:
            val = self._left if isinstance(self._left, str) else self._left.value
            names = val.split('.')
            l_op, number = None, None

        assert len(names) == 2, "Invalid column name in comparison (%s)" % self.left
        table_name, column_name = names
        for table in tables:
            if table.name == table_name or table.nickname == table_name:
                if column_name in table.column_index:
                    return Column(table, column_name, l_op, number)

        assert False, "No column was found for (%s)" % self.left

    def right_column(self, tables):
        return self.right_column_or_constant(tables, True)

    def right_column_or_constant(self, tables, force_column=False):
        """
        The comparison has not parsed the left and right to be actual columns yet

        Use the tables to find the correct table and column combination. If no
        column combination is found then assume that the right is a constant

        :param tables: list of tables
        :param force_column: fails with error if the right hand side could not be parsed as a column
        :return: Column for right hand side or unboxed constant
        """

        if type(self._right) == sqlparse.sql.Operation:
            names, l_op, number = self.parse_op(self._right)
            table_name, column_name = names
            for table in tables:
                if table.name == table_name or table.nickname == table_name:
                    if column_name in table.column_index:
                        return Column(table, column_name, l_op, number)

        val = self._right if isinstance(self._right, str) else self._right.value
        names = val.split('.')
        if len(names) == 2:
            table_name, column_name = names
            for table in tables:
                if table.name == table_name or table.nickname == table_name:
                    if column_name in table.column_index:
                        return Column(table, column_name)

        assert not force_column, "Column was required on right hand side of Comparison"

        # The right must be constant remove the double quotes
        if val[0] == "\"" and val[-1] == "\"":
            val = val[1:-1]

        # Try to parse as float, date, int, boolean, and text
        left_col = self.left_column(tables)
        return left_col.table.parse_value_for_column(val, left_col.name)

    def compares_constant(self, tables):
        return not isinstance(self.right_column_or_constant(tables), Column)

    @property
    def operator(self):
        return self._operator

    @staticmethod
    def from_like(column, pattern):
        comparison = Comparison(None)
        comparison._left = column
        comparison._right = pattern
        comparison._operator = "LIKE"
        return comparison
