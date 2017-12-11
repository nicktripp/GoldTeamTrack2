from server.query.Parser import Parser
from server.query.QueryFacade import QueryFacade
from server.query.QueryOptimizer import QueryOptimizer
from server.query.SQLParsingError import SQLParsingError
from server.query.TableProjector import TableProjector
import time

class Hangman:
    """
    Hangman is the executes SQL queries.
    """

    @staticmethod
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
            t0 = time.time()
            print("Executing query :: ", query)

            # Parse the query
            parser = Parser(query)
            parsed_query = parser.parse_select_from_where()

            # Optimize a plan for the query facade
            optimizer = QueryOptimizer(*parsed_query)

            # Execute the plan through the facade
            facade = QueryFacade(optimizer.tables)

            t1 = time.time()
            print("%f s elapsed preparing for execution" % (t1 - t0))

            results = facade.execute_plan(optimizer.projection_columns, optimizer.tables, optimizer.execution_conditions)

            # Aggregate the results
            projector = TableProjector(optimizer.tables, optimizer.projection_columns)
            query_output = projector.aggregate(results, optimizer.distinct)
            return query_output
        except SQLParsingError as e:
            return e.message
        # TODO: throw different errors and handle them
