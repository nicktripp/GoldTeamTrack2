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
        self._left = sqlparse_comparison.left.value
        self._right = sqlparse_comparison.right.value
        for token in sqlparse_comparison.tokens:
            if token.ttype == sqlparse.tokens.Comparison:
                self._operator = token.value
        assert self._operator is not None, "There must be a comparison operator"
        assert self._left is not None, "There must be a column on the left hand side of the comparison"
        assert self._right is not None, "There must be a column or constant on the right hand side of the comparison."

    def __repr__(self):
        return "%s %s %s" % (self._left, self._operator, self._right)

    @property
    def left(self):
        return self._left

    @property
    def right(self):
        return self._right

    def left_column(self, tables):
        """
        The comparison has not parsed the left and right to be actual columns yet

        Use the tables to find the correct table and column combination
        or raise and error
        :param tables: list of tables
        :return: Column for left hand side of the comparison
        """
        names = self.left.split('.')
        assert len(names) == 2, "Invalid column name in comparison (%s)" % self.left
        table_name, column_name = names
        for table in tables:
            if table.name == table_name or table.nickname == table_name:
                if column_name in table.column_index:
                    return Column(table, column_name)

        assert False, "No column was found for (%s)" % self.left

    def right_column_or_constant(self, tables):
        """
        The comparison has not pasred the left and right to be actual columns yet

        Use the tables to find the correct table and column combination. If no
        column combination is found then assume that the right is a constant

        :param tables: list of tables
        :return: Column for right hand side or unboxed constant
        """
        names = self.right.split('.')
        if len(names) == 2:
            table_name, column_name = names
            for table in tables:
                if table.name == table_name or table.nickname == table_name:
                    if column_name in table.column_index:
                        return Column(table, column_name)

        # The right must be constant remove the double quotes
        if self.right[0] == "\"" and self.right[-1] == "\"":
            self._right = self._right[1:-1]

        # Try to parse as float, date, int, boolean, and text
        return TableIndexer.parse_value(self.right)

    @property
    def compares_constant(self):
        return not isinstance(self.right_column_or_constant, Column)

    @property
    def operator(self):
        return self._operator


class LikeComparison(Comparison):
    def __init__(self, column, pattern):
        self._column = column
        self._pattern = pattern

    @property
    def column(self):
        return self._column

    @property
    def pattern(self):
        return self._pattern
