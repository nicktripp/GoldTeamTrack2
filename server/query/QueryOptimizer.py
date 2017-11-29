from server.query.QueryPlan import QueryPlan


class QueryOptimizer:
    """
    With the output from the QueryParser, create a plan for executing the query efficiently.

        `WHERE NOT (A.a < B.a OR A.a > B.a)`
        should translate to
        `WHERE NOT (A.a <> B.a)
        should translate to
        `WHERE A.a = B.a`
    """

    def __init__(self, parsed_query):
        self._parsed_query = parsed_query
        pass

    def get_plan(self):
        """
        Returns a plan to execute using the QueryFacade
        :return:
        """
        plan = QueryPlan()

        # TODO: handle JOIN with conditions
        join_plan = self._reduce_joins()

        # TODO: get the comparisons between (columns and constants) and (columns and columns)
        where_plan = self._get_where_conditions()

        # TODO: logically change comparisons (ie NOT A.a < A.b should be A.a >= A.b)
        # this is complicated
        reduced_plan = self._reduce_where_plan()

        return reduced_plan

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

    def _reduce_where_plan(self, where_plan):
        plan = QueryPlan()
        return plan

    def get_data_sources(self):
        """
        The data sources for the query are supplied in the FROM portion of the parsed_query
        :return:
        """
        # TODO: check that CSV files exist

        # TODO: return objects that describe the from table (make an object or something)
        pass

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