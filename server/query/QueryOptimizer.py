class QueryOptimizer:
    """
    With the output from the QueryParser, create a plan for executing the query efficiently.
    """

    def __init__(self, parsed_query):
        self.parsed_query = parsed_query
        pass

    def get_plan(self):
        """
        Returns a plan to execute using the QueryFacade
        :return:
        """
        # TODO: handle JOIN with conditions

        # TODO: find comparisons between column and constant

        # TODO: find comparisons between column and column

        # TODO: logically change comparisons (ie NOT A.a < A.b should be A.a >= A.b)

        # TODO: return Comparison objects in list form or something

        return None

    def get_data_sources(self):
        """
        The data sources for the query are supplied in the FROM portion of the parsed_query
        :return:
        """
        # TODO: check that CSV files exist

        # TODO: return objects that describe the from table (make an object or something)
        pass