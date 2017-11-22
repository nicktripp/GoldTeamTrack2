class TableProjector:
    """
    A TableProjector is given the csv file names.

    It can read the rows of the tables projected onto specific columns.
    """
    def __init__(self, data_sources):
        # TODO: assert that the data_sources exist in data/data_source.csv
        pass

    def aggregate(self, results):
        """
        The results that are supplied are the results of executing the plan on the query facade
        :param results: QueryFacadeResult list
        :return: query_output
        """
        # TODO: loop over results (result is probably an object)

        # TODO: handle reading and projecting from multiple tables, then concatenating for joins
        pass