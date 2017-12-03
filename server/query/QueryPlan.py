class QueryPlan:
    """
    A QueryPlan.py is constructed by the QueryOptimizer

    It contains information about where data sources are,
    what comparisons need to be performed, the order to perform
    the comparisons, and what columns will be projected in the result.

    A QueryPlan.py can contain another QueryPlan.py as a condition in order to
    support queries like WHERE A.a = B.a AND (A.b < B.b OR A.c > B.c)

    Sometimes NOTs are not optimized by the QueryOptimizer, ie
    `WHERE NOT A.a < B.a` translates to `WHERE A.a >= B.a` but

    `WHERE NOT ((A.a < B.a OR A.a > B.a) AND A.b = B.b)` translates to
    `WHERE NOT (A.a <> B.a AND A.b = B.b)
    """

    def __init__(self):
        # The conditions that will be executed in this plan
        self._comparisons = []

        # The way that conditions are related (AND, OR)
        self._conjunctions = []

        # Flag for NOT preceding this query plan
        self._is_negated = []

    def add_column_comparison(self, column1, column2, comparison, conjunction=None):
        """
        Adds a condition to the query plan
        :param column1: first column in the comparison
        :param column2: second column in the comparison
        :param comparison: the comparison to make
        :param conjunction: AND, OR logic for combining results of condition
        :return:
        """
        self._add_condition((column1, column2, comparison), conjunction)

    def add_query_plan_condition(self, sub_query_plan, conjunction=None):
        """
        Adds conditions that are in another QueryPlan.py
        The conditions in parenthesis are represented by the sub_query_plan
        ie WHERE A.a = B.b AND (A.b = B.c OR A.c = B.d)
        :param sub_query_plan: QueryPlan.py to add to this one
        :param conjunction: AND, OR logic for combining results of condition
        :return:
        """
        self._add_condition(sub_query_plan, conjunction)

    def _add_condition(self, condition, conjunction):
        """
        Private method adds condition with conjunction
        :param condition:
        :param conjunction:
        :return: None
        """
        self._conditions.append(condition)
        if conjunction is not None:
            self._conjunctions.append(conjunction)

        assert len(self._conditions) == len(self._conjunctions) + 1, "The number of conditions does not match the " \
                                                                     "number of conjunctions."

    @property
    def data_sources(self):
        return self._data_sources

    @property
    def conditions(self):
        return self._conditions

    @property
    def conjunctions(self):
        return self._conjuctions

    @property
    def is_negated(self):
        return self._is_negated

    @is_negated.setter
    def is_negated(self, value):
        assert isinstance(value, bool), "The is_negated flag must be a boolean."
        self._is_negated = value
