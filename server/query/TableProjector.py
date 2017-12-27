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
        self._math_cols = [i for i, proj_col in enumerate(self._projection_columns) if proj_col.op is not None]

        # Fill map of table to projection columns index in row of table
        self._columns_to_read_by_table = defaultdict(set)
        for col in self._projection_columns:
            if col.name == '*':
                self._columns_to_read_by_table[repr(col.table)] |= set(col.table.column_index.values())
            else:
                self._columns_to_read_by_table[repr(col.table)].add(col.table.column_index[col.name])

        # Map a table_index to the indices of the desired columns for the table in the output
        self._proj_col_idx_by_table_idx = defaultdict(list)
        self._num_projections = 0
        tc = defaultdict(int)
        for i, proj_col in enumerate(self._projection_columns):
            for j, table in enumerate(tables):
                if proj_col.table == table:
                    if proj_col.name == '*':
                        pairs = []
                        for _ in table.column_index.values():
                            pairs.append((tc[j], self._num_projections))
                            self._num_projections += 1
                            tc[j] += 1
                        self._proj_col_idx_by_table_idx[j].extend(pairs)
                    else:
                        self._proj_col_idx_by_table_idx[j].append((tc[j], self._num_projections))
                        self._num_projections += 1
                        tc[j] += 1

    def project(self, rows_generators, distinct):
        # Open the CSV tables
        table_files = self._open_files()

        # Get all the row locations that we need to read for each table
        rows = defaultdict(set)
        tups = []
        for tup in rows_generators:
            for i, v in enumerate(tup):
                rows[i].add(v)
            tups.append(tup)

        # Read the necessary rows from each table
        table_projections = defaultdict(dict)
        for table_index in rows:
            table_file = table_files[table_index]
            desired_columns = self._columns_to_read_by_table[repr(self._tables[table_index])]
            for location in sorted(list(rows[table_index])):
                # Read the row at location in table_file
                column_values = self.read_col_vals_multiline(location, table_file)

                # Select the desired columns from the row and store them according to the location as an ordered tuple
                projected_column_values = tuple(
                    column_values[i] for i in range(len(column_values)) if i in desired_columns)
                table_projections[table_index][location] = projected_column_values

        # Organize the outputs
        output = []
        for tup in tups:
            output.append([''] * self._num_projections)
            for i, v in enumerate(tup):
                col_proj = self._proj_col_idx_by_table_idx[i]
                cols = table_projections[i][v]
                for c, p in col_proj:
                    output[-1][p] = cols[c]
            for c in self._math_cols:
                output[-1][c] = str(self._projection_columns[c].transform(float(output[-1][c])))
            output[-1] = ','.join(output[-1])

        # Ensure DISTINCT output
        if distinct:
            output = list(set(output))
        return output

    def _open_files(self):
        table_files = []
        for i in range(len(self._tables)):
            table_files.append(open(self._tables[i].filename, 'r', encoding='utf8'))
        return table_files

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
