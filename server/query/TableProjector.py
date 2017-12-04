from collections import defaultdict

import os


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
        for col in self._projection_columns:
            if col.name == '*':
                self._columns_for_table[repr(col.table)].extend(col.table.column_index.values())
            else:
                self._columns_for_table[repr(col.table)].append(col.table.column_index[col.name])

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
                if not cols:
                    continue
                loc = tup[i]
                if loc not in table_projections[i]:
                    if loc == '*':
                        # Read every row
                        table_files[i].seek(0)
                        table_files[i].readline()
                        loc = table_files[i].tell()
                        size = os.path.getsize(self._tables[i].filename)
                        while loc < size:
                            # Read the row and project
                            table_files[i].seek(loc)
                            row_str = table_files[i].readline()[:-1]
                            col_vals = row_str.split(',')
                            projection = ','.join(col_vals[i] for i in cols)
                            row.append(projection)

                            # Update loc and repeat
                            loc = table_files[i].tell()
                    else:
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
