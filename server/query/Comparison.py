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

        #self._left = sqlparse_comparison.left.value
        self._left = sqlparse_comparison.left
        #self._right = sqlparse_comparison.right.value
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

    def left_column(self, tables):
        """
        The comparison has not parsed the left and right to be actual columns yet

        Use the tables to find the correct table and column combination
        or raise and error
        :param tables: list of tables
        :return: Column for left hand side of the comparison
        """

        if type(self._left) == sqlparse.sql.Operation:
            real_name = self._left.tokens[0].value
            l_op = self._left.tokens[2]
            number = self._left.tokens[4]
            names = real_name.split('.')
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
        The comparison has not pasred the left and right to be actual columns yet

        Use the tables to find the correct table and column combination. If no
        column combination is found then assume that the right is a constant

        :param tables: list of tables
        :param force_column: fails with error if the right hand side could not be parsed as a column
        :return: Column for right hand side or unboxed constant
        """

        val = self._right if isinstance(self._right, str) else self._right.value
        if '.' in val:
            names = val.split('.')
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
