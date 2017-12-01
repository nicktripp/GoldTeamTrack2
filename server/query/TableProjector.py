from collections import defaultdict


class TableProjector:
    """
    A TableProjector is given the csv file names.

    It can read the rows of the tables projected onto specific columns.
    """
    def __init__(self, data_sources, projection_columns):
        self._tables = data_sources
        self._projection_columns = projection_columns

        # Fill map of table to projection columns index in row of table
        self._columns_for_table = defaultdict(list)
        for table in self._tables:
            for col in self._projection_columns:
                if col.table == table:
                    self._columns_for_table[table].append(table.column_index[col])
        pass

    def aggregate(self, row_start_tuples):
        """
        The results that are supplied are the results of executing the plan on the query facade
        :param results: QueryFacadeResult list
        :return: query_output
        """
        if len(row_start_tuples) == 0:
            return "\n"

        # Assert the row_tuples have the correct shape
        tables_to_read = len(row_start_tuples[0])
        assert all(len(row_tup) == tables_to_read for row_tup in row_start_tuples), "Same number of tables must be read for each row"

        # Load each row for a table and project it to the desired column
        output = []
        for i in range(tables_to_read):
            # Get the indices of all of the projection columns for this table
            cols = self._columns_for_table[self._tables[i]]
            with open(self._tables[i]) as f:
                # Iterate over every row tuple for this table
                for m, tup in enumerate(row_start_tuples):
                    # Seek and Read the row
                    start = tup[i]
                    f.seek(start)
                    row = f.readline()[:-1]

                    # Project the row into the desired columns and append it to the output
                    col_vals = row.split(',')
                    projection = ','.join(col_vals[i] for i in cols)
                    # Check if the output has begun collecting for this row yet (first pass)
                    if m >= len(output): # first pass
                        output.append([projection])
                    else:                # second and on passes
                        output[m].append(projection)

        # Return a list of string rows
        return [','.join(row) for row in output]
