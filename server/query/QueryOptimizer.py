from server.query.Comparison import Comparison

class QueryOptimizer:
    """
    With the output from the QueryParser, create a plan for executing the query efficiently.

        `WHERE NOT (A.a < B.a OR A.a > B.a)`
        should translate to
        `WHERE NOT (A.a <> B.a)
        should translate to
        `WHERE A.a = B.a`
    """

    relative_path = "../../"

    def __init__(self, cols, tbls, conds, distinct):
        # String Parsings of column, table, and condition values
        self._cols = cols
        self._tbls = tbls
        self._conds = conds
        self._exec_conds = None
        self._distinct = distinct

        self._compute_execution_conditions()

    def _compute_execution_conditions(self):
        # Each item in the self.conds list is a list of ANDed comparisons
        # the results of each item are ORed together for the final result
        if not self._conds:
            self._exec_conds = []
            return

        not_flag = self._conds[0]
        for and_group in self._conds[1]:
            and_not_flag = and_group[0]
            for cond in and_group[1]:
                cond_not_flag = cond[0]
                if isinstance(cond[1], Comparison):
                    # Condition in AND group
                    pass
                elif isinstance(cond[1], list):
                    # Parenthesis of with new conditions
                    # TODO: recurse
                    pass
                else:
                    assert False, "Conditions must be a Comparison or start over."

        # TODO: actually do something
        self._exec_conds = self._conds

    @property
    def execution_conditions(self):
        return self._exec_conds

    @property
    def tables(self):
        return self._tbls

    @property
    def projection_columns(self):
        return self._cols

    @property
    def distinct(self):
        return self._distinct

    @staticmethod
    def try_parse_constant(val):
        # TODO: Strip outer quotes
        try:
            return float(val)
        except ValueError:
            try:
                return int(val)
            except ValueError:
                return val