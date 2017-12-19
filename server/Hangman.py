from server.query.Parser import Parser
from server.query.QueryFacade import QueryFacade
from server.query.QueryOptimizer import QueryOptimizer
from server.query.SQLParsingError import SQLParsingError
from server.query.TableProjector import TableProjector
import time
from functools import wraps
import sys

sys.setrecursionlimit(1000000)

def timeit(timer_tag):
    def func_wrapper(func):
        @wraps(func)
        def returned_wrapper(*args, **kwargs):
            print(timer_tag)
            t0 = time.time()
            result = func(*args, **kwargs)
            t1 = time.time()
            print("Time Elapsed :: %f s" % (t1 - t0))
            return result

        return returned_wrapper

    return func_wrapper


class Hangman:
    """
    Hangman is the executes SQL queries.
    """

    @staticmethod
    @timeit("0. Starting Query")
    def execute(query):
        """
        Parses the SQL statements
        Creates an execution plan
        Executes the plan
        Aggregates the results of the execution

        :param query: SQL query
        :return: query result
        """

        try:
            print("Executing query :: ", query)

            # Parse the query
            parsed_query = Hangman.parse(query)

            # Optimize a plan for the query facade
            optimizer = Hangman.optimize(parsed_query)

            # Execute the plan through the facade
            facade = Hangman.prepare_facade(optimizer)
            results = Hangman.execute_plan(facade, optimizer)

            # Aggregate the results
            projector = Hangman.prepare_projector(optimizer)
            query_output = Hangman.project(optimizer, projector, results)
            return query_output
        except SQLParsingError as e:
            return e.message
            # TODO: throw different errors and handle them

    @staticmethod
    @timeit("6. Projecting the Results")
    def project(optimizer, projector, results):
        query_output = projector.aggregate(results, optimizer.distinct)
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
    def prepare_facade(optimizer):
        facade = QueryFacade(optimizer.tables)
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
