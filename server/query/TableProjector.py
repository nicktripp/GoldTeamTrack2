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

        # Support projection order of columns
        self._out_col_idx_by_tbl_col_tup = {}
        i = 0
        for col in self._projection_columns:
            t = self._tables.index(col.table)
            if col.name == '*':
                for j in range(len(col.table.column_index.values())):
                    self._out_col_idx_by_tbl_col_tup[(t, j)] = i
                    i += 1
            else:
                self._out_col_idx_by_tbl_col_tup[(t, col.table.column_index[col.name])] = i
                i += 1
        self._out_columns = [[] for _ in range(i)]

    def aggregate(self, row_tup_generator, distinct):
        """
        The results that are supplied are the results of executing the plan on the query facade
        :param row_tup_generator: list or generator of tuples of row locations in csv files
        :param distinct flag for the DISTINCT keyword
        :return: query_output
        """
        # Open the CSV tables
        table_files = []
        for i in range(len(self._tables)):
            table_files.append(open(self._tables[i].filename, 'r', encoding='utf8'))

        # Iterate through the tuples
        table_projections = [{} for _ in range(len(self._tables))]
        tables_to_read = None

        for i in range(len(self._tables)):
            for tup in row_tup_generator:
                # Assert that they all read from the same number of tables
                if tables_to_read is None:
                    tables_to_read = len(tup)
                else:
                    assert len(tup) == tables_to_read, "All tuples must be of the same length for the TableProjector"

                cols = self._columns_for_table[repr(self._tables[i])]
                if not cols:
                    continue
                loc = tup[i]
                if loc not in table_projections[i]:
                    if loc is None:
                        # Read every row
                        table_files[i].seek(0)
                        table_files[i].readline()
                        loc = table_files[i].tell()
                        size = os.path.getsize(self._tables[i].filename)
                        while loc < size:
                            # Read the row while escaping text values that span multiple lines
                            col_vals = self.read_col_vals_multiline(loc, table_files[i])
                            assert len(col_vals) == len(self._tables[i].column_index)

                            # Project the column values
                            projections = [col_vals[j] for j in cols]

                            for j in cols:
                                self._out_columns[self._out_col_idx_by_tbl_col_tup[(i, j)]].append(col_vals[j])

                            # Update loc and repeat
                            loc = table_files[i].tell()
                    else:
                        # Read the row while escaping text values that span multiple lines
                        col_vals = self.read_col_vals_multiline(loc, table_files[i])
                        assert len(col_vals) == len(self._tables[i].column_index)

                        # Project the columns
                        projections = [col_vals[j] for j in cols]
                        for j, col_val in zip(cols, projections):
                            col = self._out_col_idx_by_tbl_col_tup[(i, j)]
                            self._out_columns[col].append(col_val)

                        # Save for later
                        table_projections[i][loc] = projections
                else:
                    # Look up projection
                    for j, col_val in zip(cols, table_projections[i][loc]):
                        self._out_columns[self._out_col_idx_by_tbl_col_tup[(i, j)]].append(col_val)

        output = []
        n = len(self._out_columns[0])
        for i in range(n):
            output.append(','.join(self._out_columns[j][i] for j in range(len(self._out_columns))))

        # Close the csv files
        for file in table_files:
            file.close()

        # Remove duplicate entries if DISTINCT keyword is used
        if distinct:
            output = list(set(output))

        return output

    @staticmethod
    def read_col_vals_multiline(loc, table_file):
        col_vals = []
        table_file.seek(loc)
        multiline = False
        start = True
        while start or multiline:
            start = False
            row_str = table_file.readline()
            row_split = row_str.split(',')
            for j, col_val in enumerate(row_split):
                # Handle the beginning of escaped text column value
                if TableProjector.opens_multiline(col_val, multiline):
                    if multiline:
                        # We are already in an escaped value, keep adding to it
                        col_vals[-1] += "," + col_val
                    else:
                        # We just escaped a value, add the beginning of the escape to the col_vals
                        multiline = True
                        col_vals.append(col_val)

                    # If the escaped text ends in the same col_val, it isn't multiline
                    # we are done
                    if TableProjector.closes_multiline(col_val, True):
                        multiline = False

                else:
                    # Handle closing a multiline text value on a different line or over col_val values
                    if TableProjector.closes_multiline(col_val, False):
                        col_vals[-1] += "," + col_val
                        multiline = False

                    # Handle adding more to multiline without closing it
                    elif multiline:
                        if j != 0:
                            col_vals[-1] += ","
                        col_vals[-1] += col_val

                    # Handle reading normal column values
                    else:
                        # The last split column value has an extra \n character
                        if j == len(row_split) - 1 and not multiline:
                            col_vals.append(col_val[:-1])
                        else:
                            col_vals.append(col_val)
        return col_vals

    @staticmethod
    def opens_multiline(s, already_opened):
        if len(s) == 0 or already_opened:
            return False
        if s[0] != "\"":
            return False
        if s[0] == "\"":
            q = 0
            for c in s:
                if c == "\"":
                    q += 1
                else:
                    break
            # Odd number of quotes starts a multiline
            return q % 2 == 1

    @staticmethod
    def closes_multiline(s, just_opened):
        if len(s) == 0 or (s == "\"" and just_opened):
            return False
        if s[-1] != "\"":
            return False
        if s[-1] == "\"":
            q = 0
            for c in reversed(s):
                if c == "\"":
                    q += 1
                else:
                    break
            # Odd number of quotes stops a multiline
            return q % 2 == 1
