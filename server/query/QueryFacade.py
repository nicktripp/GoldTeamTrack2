import os

from server.indexing.TableIndexer import TableIndexer
from server.query.Column import Column
from server.query.Comparison import Comparison
from server.query.EvaluationResult import EvaluationResult
from server.query.Table import Table
from server.Timer import timeit


class QueryFacade:
    """
    We will hide the table querying interface behind this class
    """

    def __init__(self, tables, condition_columns, projection_columns, indexType):
        """
        Data sources are supplied by the QueryOptimizer
        :param tables:
        """
        self._tables = tables
        self._proj_columns = projection_columns
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
                tbl_rows.append(self._table_indices[repr(tbl)].mem_locs)
            return EvaluationResult.cartesian_generator(tbl_rows)
        else:
            # Handle queries with conditions
            eval_result = self.eval_conditions(conds)
            return eval_result.generate_tuples()

    def index_for_column(self, col):
        tbl = col.table
        return self._table_indices[repr(tbl)].column_indices[col.name]

    @timeit("Evaluating Comparison")
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
            table_index = self._tables.index(comparison.left_column(self._tables).table)
            return table_index, result
        else:
            # Get the index for the right hand side of the comparison
            right_column = comparison.right_column_or_constant(self._tables)
            right_index = self.index_for_column(right_column)

            # Iterate over the values of the right index
            values = {}
            left_tup_idx = self._tables.index(comparison.left_column(self._tables).table)
            right_tup_idx = self._tables.index(comparison.right_column(self._tables).table)
            for k, vs in right_index.items():
                # Get the rows that satisfy for each table's column
                left_rows = left_index.op(k, comparison.operator, negated)
                if left_tup_idx <= right_tup_idx:
                    # return a map from the lesser table index values to the greater
                    for v in left_rows:
                        values[v] = set(vs)
                else:
                    for v in vs:
                        values[v] = set(left_rows)
            return left_tup_idx, right_tup_idx, values

    @timeit("Evaluating Conditions")
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
            eval_result = None

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
                eval_result = self._intersect_evaluation(eval_result, evaluation)

            # Negate the result
            if not_intersection:
                eval_result = self._negate(eval_result)

            # Collect the evaluation results of the OR groups
            if union_result is None:
                union_result = eval_result
            else:
                union_result = self._union_evaluation(union_result, eval_result)

        # Negate the result
        if not_union:
            union_result = self._negate(union_result)

        return union_result

    @timeit("Intersecting Evaluation")
    def _intersect_evaluation(self, eval_result, evaluation):
        """
        Can intersect the result from eval_comparison with the results from eval_conditions2
        :param eval_result: the accumulation thus far
        :param evaluation: the new values to consider
        :return: the new intersection
        """
        if eval_result is None:
            eval_result = EvaluationResult(len(self._tables))

        if isinstance(evaluation, EvaluationResult):
            # Handle the result of a nested condition statement
            return eval_result.intersect(evaluation)
        elif len(evaluation) == 2:
            # Handle the result of a column to constant comparison
            table_index, table_locations = evaluation
            eval_result.intersect_row_list(table_index, table_locations)
            return eval_result
        elif len(evaluation) == 3:
            # Handle the result of a column to column comparison
            left_index, right_index, row_map = evaluation

            if left_index + 1 == right_index:
                eval_result.intersect_consecutive_table_rows(left_index, row_map)
            else:
                eval_result.intersect_nonconsecutive_table_rows(left_index, right_index, row_map)

            return eval_result

    @timeit("Unionizing Evaluation")
    def _union_evaluation(self, union_result, eval_result):
        """
        Can union the result from eval_comparison with the results from eval_conditions2
        :param union: the accumulation thus far in a list of list of tuples
        :return: the new union
        """
        return union_result.union(eval_result)

    @timeit("Negating Results")
    def _negate(self, result):
        """
        Returns the complement of the results, using the mem_locs store by each TableIndexer
        :param results: results from prior evaluations in eval_conditions2 in list of tuple list or EvaluationResult form
        :return: the negation of results
        """
        table_mem_locs = [self._table_indices[repr(tbl)].mem_locs for tbl in self._tables]
        if isinstance(result, EvaluationResult):
            result.negate(table_mem_locs)
            return result.generate_tuples()
        elif isinstance(result, list):
            # Since there are no * generate all tuples from the cartesian product unless they are in the tuples
            return list(EvaluationResult.cartesian_generator(table_mem_locs, result))
