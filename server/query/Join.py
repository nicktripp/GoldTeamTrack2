from enum import Enum

from server.query.Comparison import Comparison

class JoinType(Enum):
    """
    These are the types of Joins we will support.
    NOOP is used for singletons.
    """
    NOOP = 0
    CROSS = 1
    JOIN = 2

class Join:
    """
    Holds information about the tables to be joined.
    A Join instance has a left and a right side, a join operator, and a condition.

    @:param left - the left side of the JOIN statement
    @:param right - the right side of the JOIN statement, or None if the JOIN is a singleton.
    @:param operator - the operator joining the left and right sides.  Can be CROSS or JOIN, or NOOP if singleton
    @:param condition - the condition upon which to JOIN the tables. See JoinType.
    """

    def __init__(self, left, operator, right=None, condition=None):
        self.left = left
        self.right = right
        self.operator = operator
        self.is_singleton = False
        self.condition = condition

        if (self.right is None):
            assert type(self.left) is str, "If no right hand table, left must be a single string"
            self.is_singleton = True
        else:
            assert type(self.left) is Join, "Left hand side must be a Join"
            assert type(self.right) is Join, "Right hand side must be a Join"

        assert self.left is not None, "Left hand table must exist"
        assert type(self.operator) is JoinType, "Operator must be specified as a JoinType"

        if(self.condition is not None):
            assert type(self.condition) is Comparison, "Condition must be an '=' comparison"
            assert self.condition.operator == "=", "Condition must be an '=' comparison"

    def set_conditon(self, condition):
        self.condition = condition
        if(self.condition is not None):
            assert type(self.condition) is Comparison, "Condition must be an '=' comparison"
            assert self.condition.operator == "=", "Condition must be an '=' comparison"

    @property
    def left(self):
        return self.left

    @property
    def right(self):
        return self.right

    @property
    def operator(self):
        return self.operator

    @property
    def is_singleton(self):
        return self.is_singleton

    @property
    def condition(self):
        return self.condition


class SingletonJoin(Join):
    """
    Holds a single table in a string in self.left.
    """

    def __init__(self, left):
        self.left = left
        self.right = None
        self.operator = JoinType.NOOP
        self.is_singleton = True
        self.condition = False
        assert type(self.left) is str, "Singleton left-side must be a string"

