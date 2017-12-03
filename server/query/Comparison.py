import sqlparse


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
