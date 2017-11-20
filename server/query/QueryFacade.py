from os import listdir
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
        indices = {}
        for column in select_columns:
            index = FileIndexer.get_indexer(relative_path, *column.split('.'))
            indices[column] = index

        # Order the conditions that will be executed
        column_column_args, column_constant_args = QueryFacade.get_condition_args(indices, where_conditions)

        print(column_column_args)
        print(column_constant_args)

        # Identify the csv files that we will query
        for table in from_tables:
            print(table)

        # Identify the where conditions
        for condition in where_conditions:
            print(condition)

        return "SELECT " + str(select_columns) + " FROM " + str(from_tables) + " WHERE " + str(where_conditions) + \
               "\nSELECT " + str(select_columns.__class__) + " FROM " + str(from_tables.__class__) + " WHERE " + str(
            where_conditions.__class__)

    @staticmethod
    def get_condition_args(indices, where_conditions):
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
                return val
