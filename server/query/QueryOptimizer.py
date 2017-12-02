import os

from server.query.Column import Column
from server.query.QueryPlan import QueryPlan
from server.query.Table import Table


class QueryOptimizer:
    """
    With the output from the QueryParser, create a plan for executing the query efficiently.

        `WHERE NOT (A.a < B.a OR A.a > B.a)`
        should translate to
        `WHERE NOT (A.a <> B.a)
        should translate to
        `WHERE A.a = B.a`
    """

    def __init__(self, cols, tbls, conds, relative_path="../../"):
        # String Parsings of column, table, and condition values
        self._cols = cols
        self._tbls = tbls
        self._conds = conds

        # self._parsed_query = parsed_query

        self._relative_path = relative_path

        self._tables = []
        self._projection_columns = []

    def get_plan(self):
        """
        Returns a plan to execute using the QueryFacade
        :return:
        """
        plan = QueryPlan()

        # TODO: handle JOIN with conditions
        join_plan = self._reduce_joins()

        # TODO: set the _tables
        self._get_tables()

        # TODO: make Column objects for each column in SELECT
        self._get_projections()

        # TODO: get the comparisons between (columns and constants) and (columns and columns)
        where_plan = self._get_where_conditions()

        # TODO: logically change comparisons (ie NOT A.a < A.b should be A.a >= A.b)
        # this is complicated
        reduced_plan = self._reduce_where_plan(join_plan, where_plan)

        return reduced_plan

    def _get_tables(self):
        self._tables = [Table(self._relative_path + "./movies.csv")]
        assert len(self._tables) > 0, "There must be at least one source table."

    def _get_projections(self):
        self._projection_columns = []
        for column in self._parsed_query.select_columns:
            if self._is_aggregate(column):
                # TODO: handle aggregate functions like COUNT(*)
                pass
            else:
                # TODO: Find table name and column name from parse
                table = self._tables[0]
                self._projection_columns.append(Column(table, column))

    def _reduce_joins(self):
        """
        Remove the JOIN ON stuff from the FROM clause using conditions and stuff

        :return: a QueryPlan with the conditions for the JOIN ON
        """
        plan = QueryPlan()
        from_tables = self._parsed_query.from_tables
        for join_on in from_tables:
            table1 = join_on.first_table
            table2 = join_on.second_table
            # TODO: this is all pseudo code
            for condition in join_on.conditions:
                args = (table1, condition.column1), (table2, condition.column2), condition.comparison, condition.logic
                plan.add_column_comparison(*args)
        return plan

    def _get_where_conditions(self):
        plan = QueryPlan()

        # TODO: find comparisons between column and constant
        # use this to add column and constant comparisons
        table1, table2 = 'Table1', 'Table2'
        condition = {} # IDK what conditions will look like out of the parser
        constant = "HelloWorld"
        args = (table1, condition.column1), QueryOptimizer.try_parse_constant(constant), condition.comparison, condition.logic
        plan.add_column_comparison(*args)

        # TODO: find comparisons between column and column
        # use this to add column and column comparisons
        args = (table1, condition.column1), (table2, condition.column2), condition.comparison, condition.logic
        plan.add_column_comparison(*args)

        # TODO: IDK how the parenthesis handle sub queries and conjuctions and stuff
        where_conditions = self._parsed_query.where_conditions
        conjunctions = self._parsed_query.conjunctions
        for condition in where_conditions:
            pass

        return plan

    def _reduce_where_plan(self, join_plan, where_plan):
        # TODO: logically reduce the query conditions
        plan = QueryPlan()
        return plan

    @property
    def tables(self):
        """
        The data sources for the query are supplied in the FROM portion of the parsed_query
        :return: list of string filenames
        """
        return self._tables

    @property
    def projection_columns(self):
        return self._projections_columns

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