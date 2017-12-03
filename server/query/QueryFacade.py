import os

from server.query.Table import Table


class QueryFacade:
    """
    We will hide the table querying interface behind this class
    """

    def __init__(self, tables):
        """
        Data sources are supplied by the QueryOptimizer
        :param tables:
        """
        self._tables = tables

        if not QueryFacade.is_query_indexed(self._tables):
            # TODO: Index the tables
            pass

    @staticmethod
    def is_query_indexed(tbls):
        """
        Checks to see if the tables have been indexed with the index saved to the table_name.idx
        :param tbls: tables that need to have an index
        :return: boolean
        """
        for tbl in tbls:
            if not os.path.exists(Table.relative_path + tbl.name + '.idx'):
                return False
        return True

    def execute_plan(self, cols, tbls, conds):
        """
        Executes a QueryPlan and returns a list of list of row start locations

        The results are the locations of read in the corresponding tables as passed
        in the constructor.

        results[i][0] corresponds to the ith query result's entire row location in
        the csv file for the 0th table.
        [
            [1234, 1234, 23456],
            [2435, 3456, 3456],
            ...
        ]
        :param plan:
        :return:
        """
        if not conds:  # not conds is the same as conds == []
            # There are no conditions get all rows
            # Use the first column to get all of the row locations from an index
            tbl_rows = []
            for tbl in tbls:
                # Get a column for this table
                col = None
                for c in cols:
                    if c.table == tbl:
                        col = c
                        break

                # Get the index for the column
                col_idx = QueryFacade.index_for_column(col)

                # Collect all of the row start locations for each unique value of the column
                rows = []
                for k, vs in col_idx.items():
                    if isinstance(vs, list) or isinstance(vs, set):
                        for v in vs:
                            rows.append(v)
                    assert False, "Index should have value keys and list or set values"

                # Keep rows to make cartesian product
                tbl_rows.append(rows)

                # Return cartesian product generator
            return QueryFacade.cartesian_generator(tbl_rows)

        for group in conds:
            # Check conditions over the indexes
            pass

        return []

    @staticmethod
    def index_for_column(col):
        pass

    @staticmethod
    def cartesian_generator(tbl_rows):
        """
        tbl_rows is a list of lists of row start locations

        Generate the cartesian product of tbl_rows
        :param tbl_rows: list of lists of row locations
        """
        idx = [0] * len(tbl_rows)
        while idx[0] < len(tbl_rows[0]):
            # build row tuple in list
            row_list = []

            # get the ith value of the next cartesian tuple
            for i in range(len(tbl_rows)):
                row_list.append(tbl_rows[i][idx[i]])

            # move the index of the cartesian tuple
            for i in reversed(range(1, len(tbl_rows))):
                idx[i] += 1
                # wrap the index except for the very first list in tbl_rows
                if idx == len(tbl_rows[i]):
                    idx[i] = 0

            yield tuple(row_list)
