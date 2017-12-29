import sys

from server.Timer import timeit
from server.indexing.BTreeIndex import BTreeIndex
from server.query.Parser import Parser
from server.query.QueryFacade import QueryFacade
from server.query.QueryOptimizer import QueryOptimizer
from server.query.SQLParsingError import SQLParsingError
from server.query.TableProjector import TableProjector

sys.setrecursionlimit(1000000)


class Hangman:
    """
    Hangman is the executes SQL queries.
    """

    @staticmethod
    # @timeit("0. Starting Query")
    def execute(query, index_type=BTreeIndex):
        """
        Parses the SQL statements
        Creates an execution plan
        Executes the plan
        Aggregates the results of the execution

        :param query: SQL query
        :param index_type: class of indextype. One of:
            - BTreeIndex
            - BitmapIndex
        :return: query result
        """

        try:
            print("Executing query :: ", query)

            # Parse the query
            parsed_query = Hangman.parse(query)

            # Optimize a plan for the query facade
            optimizer = Hangman.optimize(parsed_query)

            # Execute the plan through the facade
            facade = Hangman.prepare_facade(optimizer, index_type)
            results = Hangman.execute_plan(facade, optimizer)

            # Aggregate the results
            projector = Hangman.prepare_projector(optimizer)
            query_output = Hangman.project(optimizer, projector, results)
            return query_output

        except SQLParsingError as e:

            print(e)
            # TODO: throw different errors and handle them
            raise

    @staticmethod
    @timeit("6. Projecting the Results")
    def project(optimizer, projector, results):
        query_output = projector.project(results, optimizer.distinct)
        return query_output

    @staticmethod
    @timeit("5. Preparing the Projector")
    def prepare_projector(optimizer):
        projector = TableProjector(optimizer.tables, optimizer.projection_columns)
        return projector

    @staticmethod
    @timeit("4. Executing the Plan")
    def execute_plan(facade, optimizer):
        results = facade.execute_plan(optimizer.projection_columns, optimizer.tables, optimizer.execution_conditions)
        return results

    @staticmethod
    @timeit("3. Preparing the QueryFacade")
    def prepare_facade(optimizer, index_type):
        facade = QueryFacade(optimizer.tables, optimizer.required_cols, optimizer.projection_columns, index_type)
        return facade

    @staticmethod
    @timeit("2. Optimizing Query")
    def optimize(parsed_query):
        optimizer = QueryOptimizer(*parsed_query)
        return optimizer

    @staticmethod
    @timeit("1. Parsing Query")
    def parse(query):
        parser = Parser(query)
        parsed_query = parser.parse_select_from_where()
        return parsed_query
