from collections import defaultdict


class TableProjector:
    """
    A TableProjector is given the csv file names.

    It can read the rows of the tables projected onto specific columns.
    """
    def __init__(self, tables, projection_columns):
        self._tables = tables
        self._projection_columns = projection_columns

        # Fill map of table to projection columns index in row of table
        self._columns_for_table = defaultdict(list)
        for table in self._tables:
            for col in self._projection_columns:
                if col.table == table:
                    self._columns_for_table[repr(table)].append(table.column_index[col.name])

    def aggregate(self, row_tup_generator, distinct):
        """
        The results that are supplied are the results of executing the plan on the query facade
        :param row_start_tuples: list of tuples of row locations in csv files
        :param distinct flag for the DISTINCT keyword
        :return: query_output
        """
        # Open the CSV tables
        table_files = []
        for i in range(len(self._tables)):
            table_files.append(open(self._tables[i].filename, 'r'))

        # Iterate through the tuples
        table_projections = [{}] * len(self._tables)
        tables_to_read = None
        output = []
        for tup in row_tup_generator:
            # Assert that they all read from the same number of tables
            if tables_to_read is None:
                tables_to_read = len(tup)
            else:
                assert len(tup) == tables_to_read, "All tuples must be of the same length for the TableProjector"

            # Project the row for each table if it hasn't been done yet
            row = []
            for i in range(len(self._tables)):
                cols = self._columns_for_table[repr(self._tables[i])]
                loc = tup[i]
                if loc not in table_projections[i]:
                    # Read the row and project
                    table_files[i].seek(loc)
                    row_str = table_files[i].readline()[:-1]
                    col_vals = row_str.split(',')
                    projection = ','.join(col_vals[i] for i in cols)
                    row.append(projection)

                    # Save for later
                    table_projections[i][loc] = projection
                else:
                    # Look up projection
                    row.append(table_projections[i][loc])

            # Concatenate and append to the query result
            output.append(','.join(row))

        # Close the csv files
        for file in table_files:
            file.close()

        # Remove duplicate entries if DISTINCT keyword is used
        if distinct:
            output = list(set(output))

        return output





        # Load each row for a table and project it to the desired column
        output = []
        for i in range(tables_to_read):
            # Get the indices of all of the projection columns for this table
            cols = self._columns_for_table[repr(self._tables[i])]
            with open(self._tables[i].name, 'r') as f:
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

        # Return a list of string rows, respect the DISTINCT keyword
        rows = [','.join(row) for row in output]
        if distinct:
            return list(set(rows))
        else:
            return rows