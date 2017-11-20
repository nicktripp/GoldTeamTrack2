from os import listdir

import itertools
from sqlparse.tokens import Whitespace

from server.indexing.FileIndexer import FileIndexer


class QueryFacade:
    """
    We will hide the table querying interface behind this class
    """

    def __init__(self):
        pass

    @staticmethod
    def prepare(relative_path):
        files = listdir('../../../data/')
        files = [fn for fn in files if fn.endswith('.csv')]

        for csv_file in files:
            table_name = csv_file.split('/')[-1]
            table_name = table_name[:-4]
            FileIndexer(relative_path, table_name)

    @staticmethod
    def query(select_columns, from_tables, where_conditions, relative_path = '../../../'):
        """
        @param select_columns - a list of strings corresponding to the proper column name
        @param from_tables - a list of table filenames
        @param where_conditions - a list of conditions corresponding to the proper conditions
        """
        # Load the indices that we need to use
        file_indexers = {}
        for table in from_tables:
            file_indexers[table] = FileIndexer(relative_path, table)

        # Get the conditions that we are going to execute
        column_column_args, column_constant_args = QueryFacade.get_condition_args(file_indexers, where_conditions)

        # TODO: Get the logic between each condition

        # Do the single table queries
        records = None
        for args in column_constant_args:
            # these are the args that get_condition_args produced
            table, column = QueryFacade.get_table_and_column_for_select(args[0])
            table_indexer = file_indexers[table]
            index = table_indexer[column]

            # Defer comparison to index implementation
            # args[1] is a constant, args[2] is the comparisons string ie '<'
            # TODO: Consider use of NOT
            record_set = index.op(QueryFacade.try_parse_constant(args[1]), args[2])

            # Keep records that pass other conditions
            if records is None:
                records = record_set
            else:
                records = records.intersection(record_set)

        # Do the join queries
        cartesian_records = None
        for args in column_column_args:
            # Get the first column index
            table1, column1 = QueryFacade.get_table_and_column_for_select(args[0])
            table1_indexer = file_indexers[table1]
            index1 = table1_indexer[column1]

            # Get the second column index
            table2, column2 = QueryFacade.get_table_and_column_for_select(args[1])
            table2_indexer = file_indexers[table2]
            index2 = table2_indexer[column2]

            # Perform the comparison for all mn combinations of m values in col1 and n values of col2
            for key in index1.items():
                # Get records from first table
                # TODO: Consider use of NOT
                left_records = index1.op(key, '=')

                # Get matching records in second table
                right_records = index2.op(key, args[2])

                # We need the cartesian product of these as tuples
                cartesian = itertools.product(left_records, right_records)

                # Keep records that pass other conditions
                if cartesian_records is None:
                    cartesian_records =  cartesian
                else:
                    # TODO: Use OR or AND
                    cartesian_records = cartesian_records.intersection(cartesian)


        # TODO: if cartesian_records is not empty filter it with rows of records that passed the constant constraints
        return file_indexer.read_and_project(records, select_columns)

    @staticmethod
    def get_condition_args(indices, where_conditions):
        # TODO: change to work with FileIndexer
        column_column_args = []
        column_constant_args = []
        for condition in where_conditions:
            print(condition.tokens)
            tokens = [t for t in condition.tokens if t._get_repr_name() != 'Whitespace']
            print(tokens)
            column = str(tokens[0])
            comparison = str(tokens[1])
            other = str(tokens[2])
            assert(column in indices)
            if other in indices:
                column_column_args.append((column, other, comparison))
            else:
                constant = QueryFacade.try_parse_constant(other)
                column_constant_args.append((column, constant, comparison))
        return column_column_args, column_constant_args


    @staticmethod
    def try_parse_constant(val):
        # TODO: Strip outer quotes
        try:
            return float(val)
        except ValueError:
            try:
                return int(val)
            except ValueError:
<<<<<<< HEAD
                return val
=======
                return val

    @staticmethod
    def get_table_and_column_for_select(select_column):
        return select_column.split('.')[:2]
>>>>>>> 5188f134fc371c7104b9f0dde0fbdba54bc0ea41
