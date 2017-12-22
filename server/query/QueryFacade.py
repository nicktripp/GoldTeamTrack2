import os

from server.indexing.TableIndexer import TableIndexer
from server.query.Column import Column
from server.query.Comparison import Comparison
from server.query.Table import Table


class QueryFacade:
    """
    We will hide the table querying interface behind this class
    """

    def __init__(self, tables, condition_columns, indexType):
        """
        Data sources are supplied by the QueryOptimizer
        :param tables:
        """
        self._tables = tables
        self._table_indices = {}
        for tbl in self._tables:
            tbl_cols = [col for col in condition_columns if col.table == tbl]
            self._table_indices[repr(tbl)] = TableIndexer(tbl, tbl_cols, index_class=indexType)

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

                # Get the index for a single column
                if col.name == '*':
                    col_name = list(col.table.column_index.keys())[0]
                    col_idx = self.index_for_column(Column(col.table, col_name))
                else:
                    col_idx = self.index_for_column(col)

                # Collect all of the row start locations for each unique value of the column
                rows = []
                for k, vs in col_idx.items():
                    if isinstance(vs, list) or isinstance(vs, set):
                        for v in vs:
                            rows.append(v)
                    else:
                        assert False, "Index should have value keys and list or set values"

                # Keep rows to make cartesian product
                tbl_rows.append(rows)

                # Return cartesian product generator
            return QueryFacade.cartesian_generator(tbl_rows)
        else:
            # Handle queries with conditions
            return self.eval_conditions(conds)

    def index_for_column(self, col):
        tbl = col.table
        return self._table_indices[repr(tbl)].column_indices[col.name]

    def eval_conditions(self, conditions):
        acc_list = []
        not_flag = conditions[0]
        for group in conditions[1]:

            # Find the rows that pass each AND condition
            acc = None
            and_not_flag = group[0]
            for condition in group[1]:
                cond_not_flag = condition[0]
                if isinstance(condition[1], Comparison):
                    # Evaluate the comparison on an index or on over multiple indices
                    result = self.eval_comparison(condition[1], cond_not_flag)
                else:
                    result = self.eval_conditions(condition)

                # Accumulate an intersection over the results of the condition
                if acc is None:
                    acc = result
                acc = self._intersect_tuples(acc, result)

            # Handle NOT (A = b AND B = c)
            if and_not_flag:
                acc = self._negate_tuples(acc)

            acc_list.append(acc)

        # union (OR) the results of the conditions
        union = self._sorted_tuple_union(acc_list)
        if not_flag:
            union = self._negate_tuples(union)
        return union

    def eval_comparison(self, comparison, negated):
        """
        Performs the comparison by looking up the proper indexes for the left and right hand side of the comparison.
        Handles constant comparisons in a separate case
        :param negated: flag for NOT flag used on condition
        :param comparison:
        :return:
        """
        # Get the index for the left hand side of the comparison
        left_column = comparison.left_column(self._tables)
        left_index = self.index_for_column(left_column)

        # This is a column vs constant comparison
        if comparison.compares_constant(self._tables):
            # Get the constant on the right
            right_constant = comparison.right_column_or_constant(self._tables)

            # Perform the comparison over the index
            result = left_index.op(right_constant, comparison.operator, negated)

            # Reduce all of the key to set(location) entries to a set of tuples with the locations
            reduced = set()
            table_index = self._tables.index(comparison.left_column(self._tables).table)
            for v in result:
                # These are tuples for projections with the location set for this column's table index
                result_list = ['*'] * len(self._tables)
                result_list[table_index] = v
                reduced.add(tuple(result_list))
            return reduced
        else:
            # Get the index for the right hand side of the comparison
            right_column = comparison.right_column_or_constant(self._tables)
            right_index = self.index_for_column(right_column)

            # Iterate over the values of the right index
            reduced = set()
            left_tup_idx = self._tables.index(comparison.left_column(self._tables).table)
            right_tup_idx = self._tables.index(comparison.right_column(self._tables).table)
            for k, vs in right_index.items():
                # Get the rows that satisfy for each table's column
                right_rows = list(vs)
                left_rows = left_index.op(k, comparison.operator, negated)

                # Get the Cartesian product of the rows and insert '*' for the other columns
                for pair in self.cartesian_generator([left_rows, right_rows]):
                    result_list = ['*'] * len(self._tables)
                    result_list[left_tup_idx] = pair[0]
                    result_list[right_tup_idx] = pair[1]
                    reduced.add(tuple(result_list))
            return reduced

    @staticmethod
    def _intersect_tuples(acc, new):
        intersection = set()
        for t1 in acc:
            # If the tuple exactly exists in both keep it
            if t1 in new:
                intersection.add(t1)
                continue

            # If they disagree on a "*" field reduce it to the other tuple's value
            for t2 in new:
                t3 = []
                keep = True
                for a, b in zip(t1, t2):
                    if a == b:
                        t3.append(a)
                    elif a == "*":
                        t3.append(b)
                    elif b == "*":
                        t3.append(a)
                    else:
                        keep = False
                        break
                if keep:
                    intersection.add(tuple(t3))
        return intersection

    def _sorted_tuple_union(self, tuples):
        union = set()

        # Find all of the columns that will be '*'
        mask = [False] * len(self._tables)
        for ts in tuples:
            for t in ts:
                for i, m in enumerate(mask):
                    mask[i] = t == '*' or mask[i]

        # Union the tuples replacing the * columns iteratively
        for ts in tuples:
            for t in ts:
                nt = []
                for i, ti in enumerate(t):
                    nt.append('*' if mask[i] else ti)
                union.add(tuple(nt))

        return sorted(list(union), key=lambda x: float(x) if isinstance(x, int) else float('inf'))

    def _negate_tuples(self, tuples):
        # If any columns are * then none of then no tuples should be generated
        for t in tuples:
            for i, ti in enumerate(t):
                if ti == "*":
                    return []

        # Since there are no * generate all tuples from the cartesian product unless they are in the tuples
        return list(self.cartesian_generator([self._table_indices[repr(tbl)].mem_locs for tbl in self._tables], tuples))

    @staticmethod
    def cartesian_generator(tbl_rows, skip_tuples=set()):
        """
        tbl_rows is a list of lists of row start locations

        Generate the cartesian product of tbl_rows
        :param skip_tuples: tuples in this set will not be yielded
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
            for i in reversed(range(len(tbl_rows))):
                idx[i] += 1
                # wrap the index except for the very first list in tbl_rows
                if idx[i] == len(tbl_rows[i]) and i != 0:
                    idx[i] = 0
                else:
                    break

            # don't yield tuples in the the skip_tuples set
            tup = tuple(row_list)
            if tup not in skip_tuples:
                yield tup
