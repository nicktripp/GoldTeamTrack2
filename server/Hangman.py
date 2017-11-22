from server.query.Parser import Parser
from server.query.QueryFacade import QueryFacade
from server.query.QueryOptimizer import QueryOptimizer
from server.query.SQLParsingError import SQLParsingError
from server.query.TableProjector import TableProjector


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
            # Parse the query
            parser = Parser(query)
            parsed_query = parser.parse_select_from_where()

            # Optimize a plan for the query facade
            optimizer = QueryOptimizer(parsed_query)
            data_sources = optimizer.get_data_sources()
            execution_plan = optimizer.get_plan()

            # Execute the plan through the facade
            facade = QueryFacade(data_sources)
            results = []
            for step in execution_plan:
                results.append(facade.execute(step))

            # Aggregate the results
            projector = TableProjector(data_sources)
            query_output = projector.aggregate(results)
            return query_output
        except SQLParsingError as e:
            return e.message
        # TODO: throw different errors and handle them

