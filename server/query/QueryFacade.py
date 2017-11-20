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
        file_indexers = {}
        for table in from_tables:
            file_indexers[table] = FileIndexer(relative_path, table)

        # Get the conditions that we are going to execute
        column_column_args, column_constant_args = QueryFacade.get_condition_args(file_indexers, where_conditions)

        print(column_column_args)
        print(column_constant_args)

        result = self.do_query(file_indexers, column_column_args, column_constant_args)

        return "FAILURE"

    def do_query(self, file_indexer, col_col_conditions, col_const_conditions):
        # Only need on index to query against
        records = None
        for args in col_const_conditions:
            # these are the args that get_condition_args produced
            # TODO : idk how to do this after Nick's latest changes
            table_column = args[0].split('.')
            table_indexer = file_indexer[table_column[0]]
            index = table_indexer[table_column[1]]

            # Defer comparison to index implementation
            # args[1] is a constant, args[2] is the comparisons string ie '<'
            record_set = index.op(args[1], args[2])

            # Keep records that pass other conditions
            if records is None:
                records = record_set
            else:
                records = records.intersection(record_set)

        for args in col_col_conditions:
            # Get index for each table column
            # TODO: Still don't know how to do this
            index1 = indices[args[0]]
            index2 = indices[args[1]]

            # Perform the comparison for all mn combinations of m values in col1 and n values of col2
            # TODO: Move multi_op from BTreeIndex to FileIndexer
            for key in index1.items():
                # Use the other index to
                record_set = index2.op(key, args[2])

                # Keep records that pass other conditions
                if records is None:
                    records = record_set
                else:
                    records = records.intersection(record_set)

        # TODO: Read the rows of the tables that passed the conditions
        # rows = read_records(records, tables)

        # TODO: Project the rows into the desired columns
        #return project(rows, columns)

    def do_join_query(self, indices, col_col_conditions, col_const_conditions):
        """
        :param indices:
        :param col_col_conditions: column to column comparisons that may be across tables
        :param col_const_conditions:
        :return:
        """

        # TODO: Perform comparisons the same way, but get the row locations for both files

        # TODO: Read row locations from both tables, project, and concatenate to single row


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
                return val
