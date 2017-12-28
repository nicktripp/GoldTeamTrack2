import os
# from multiprocessing.pool import Pool

from server.indexing.TableIndexer import TableIndexer
from server.query.Comparison import Comparison
from server.query.ResultGenerator import ResultGenerator
from server.query.Table import Table


class QueryFacade:
    """
    We will hide the table querying interface behind this class
    """

    def __init__(self, tables, condition_columns, projection_columns, indexType):
        """
        Data sources are supplied by the QueryOptimizer
        :param tables:
        """
        # self._pool = Pool(4)

        self._tables = tables
        self._proj_columns = projection_columns
        self._table_indices = {}

        for tbl in self._tables:
            tbl_cols = [col for col in condition_columns if col.table == tbl]
            self._table_indices[repr(tbl)] = TableIndexer(tbl, tbl_cols, index_class=indexType)

    # def close_pool(self):
    #     self._pool.close()

    @staticmethod
    def is_query_indexed(tables):
        """
        Checks to see if the tables have been indexed with the index saved to the table_name.idx
        :param tables: tables that need to have an index
        :return: boolean
        """
        for tbl in tables:
            if not os.path.exists(Table.relative_path + tbl.name + '.idx'):
                return False
        return True

    def index_for_column(self, col):
        tbl = col.table
        return self._table_indices[repr(tbl)].column_indices[col.name]

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
                tbl_rows.append(self._table_indices[repr(tbl)].mem_locs)
            result = ResultGenerator(len(tbls), self.get_mem_locs(), [table.filename for table in self._tables])
            return result.generate_tuples()
        else:
            # Handle queries with conditions
            result = self.eval_conditions(conds)
            return result.generate_tuples()

    # @timeit("Evaluating Comparison")
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
            right_constant = comparison.left_column(self._tables).invert_transform(right_constant)

            # Perform the comparison over the index
            single_generator = left_index.op(right_constant, comparison.operator, negated)

            # Reduce all of the key to set(location) entries to a set of tuples with the locations
            table_index = self._tables.index(comparison.left_column(self._tables).table)
            return table_index, single_generator
        else:
            # Get the index for the right hand side of the comparison
            right_column = comparison.right_column_or_constant(self._tables)
            right_index = self.index_for_column(right_column)

            # Iterate over the values of the right index
            left_tup_idx = self._tables.index(comparison.left_column(self._tables).table)
            right_tup_idx = self._tables.index(comparison.right_column(self._tables).table)
            double_generator = self.columns_comparison(left_index, left_tup_idx, right_index, right_tup_idx,
                                                       left_column, right_column, comparison, negated)
            return left_tup_idx, right_tup_idx, double_generator

    def columns_comparison(self, left_index, left_tup_idx, right_index, right_tup_idx, left_column, right_column,
                           comparison,
                           negated):
        # Loop over the values of the index with fewer items
        if sum(1 for _ in right_index.items()) > sum(1 for _ in left_index.items()):
            left_index, right_index = right_index, left_index
            left_tup_idx, right_tup_idx = right_tup_idx, left_tup_idx
            left_column, right_column = right_column, left_column

        # For each key values pair in right_index
        right_items = [(k, vs) for k, vs in right_index.items()]
        for k, vs in right_items:
            # Transform key if column was given math requirements ie S.a + 5
            k = right_column.transform(k)

            # Get the row pairs that satisfy for each table's column
            left_rows = left_index.op(k, comparison.operator, negated)
            if left_tup_idx <= right_tup_idx:
                for lr in left_rows:
                    for v in vs:
                        yield (lr, v)
            else:
                for v in vs:
                    for lr in left_rows:
                        yield (v, lr)

    def eval_conditions(self, conditions):
        """
        Produces a map from table index to list of locations and
        a map from pairs of table indexes to list of pairs of table locations

        These maps can be used to compute the intersection, union, and tuple projections for the rows
        output by the query
        :param conditions:
        :return:
        """
        # Flag to negate all results and return the complement
        not_union = conditions[0]

        # variables to store the evaluation aggregates
        union_result = None

        # Iterate over each OR group in the conditions
        for group in conditions[1]:
            not_intersection = group[0]  # Flag to negate the results of the this AND group
            result_generator = ResultGenerator(len(self._tables), self.get_mem_locs(),
                                               [table.filename for table in self._tables])

            # Iterate through each ANDed condition
            for condition in group[1]:
                not_condition = condition[0]  # Flag to negate the results of this condition

                if isinstance(condition[1], Comparison):
                    # Evaluate the comparison
                    evaluation = self.eval_comparison(condition[1], not_condition)
                else:
                    # Evaluate nested conditions (recurse)
                    evaluation = self.eval_conditions(condition)

                # Consolidate the evaluation results
                self._intersect_evaluation(result_generator, evaluation)

            # Negate the result
            if not_intersection:
                result_generator.negate()

            # Collect the evaluation results of the OR groups
            if union_result is None:
                union_result = result_generator
            else:
                union_result |= result_generator

        # Negate the result
        if not_union:
            union_result.negate()

        return union_result

    def _intersect_evaluation(self, result_generator, evaluation):
        """
        Can intersect the result from eval_comparison with the results from eval_conditions2
        :param eval_result: the accumulation thus far
        :param evaluation: the new values to consider
        :return: the new intersection
        """
        if isinstance(evaluation, ResultGenerator):
            # Handle the result of a nested condition statement
            result_generator &= evaluation
        elif len(evaluation) == 2:
            # Handle the result of a column to constant comparison
            table_index, single_generator = evaluation
            result_generator.reduce_single_constraints(table_index, single_generator)
        elif len(evaluation) == 3:
            # Handle the result of a column to column comparison
            left_index, right_index, double_generator = evaluation
            if left_index == right_index:
                values = [i for i, j in double_generator if i == j]
                result_generator.reduce_single_constraints(left_index, values)
            else:
                result_generator.reduce_double_constraints((left_index, right_index), double_generator)

    def _negate(self, result_generator):
        """
        Returns the complement of the results, using the mem_locs store by each TableIndexer
        :param results: results from prior evaluations in eval_conditions2 in list of tuple list or EvaluationResult form
        :return: the negation of results
        """

    def get_mem_locs(self):
        return [self._table_indices[repr(tbl)].mem_locs for tbl in self._tables]
