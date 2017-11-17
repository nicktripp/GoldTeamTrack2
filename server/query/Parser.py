import sqlparse

from server.query.QueryFacade import QueryFacade

class Parser:
    """
    Parses an SQL query
    """

    def __init__(self, query_string):
        """
        Loads a new query into the parser.
        """
        self.query_string = query_string
        self.statements = sqlparse.parse(query_string)

    @staticmethod
    def validate(stmt):
        """
        Validates the query to check if it is valid SQL.
        Returns an FALSE if it is invalid SQL.
        """

        return stmt.get_type() != u'UNKNOWN' # Check if query begins with a valid DDL or DML keyword

        # TODO more robust validation



    @staticmethod
    def validate_select(statement):
        """
        Validates the query to check if it is a SELECT statement.
        This method checks only the first three tokens to see if they are a select operator, whitespace, and an identifier.
        """
        is_valid = statement.get_type == u'SELECT'
        is_valid = is_valid and statement.tokens[1].match(sqlparse.tokens.Whitespace,' ',regex=True) # TODO: complex whitespace matching
        is_valid = is_valid and \
            (
                statement.tokens[2].match(sqlparse.tokens.Wildcard, '*') or \
                type(statement.tokens[2]) == sqlparse.sql.Identifier or \
                type(statement.tokens[2]) == sqlparse.sql.IdentifierList
            )

        # TODO throw error here
        return is_valid



    def select_cols(self, stmt):
        """
        pulls out the columns selected in SELECT cols
        """
        self.validate_select(stmt)

        return stmt.tokens[2]



    def validate_from(self, statement):
        """
        Validates the query to check if it is a FROM statement.
        This method checks only FROM tokens to see if they are a FROM keyword, whitespace, and an identifier.
        """
        is_valid = \
            statement.tokens[3].match(sqlparse.tokens.Whitespace,' ',regex=True) and \
            statement.tokens[4].match(sqlparse.tokens.Keyword,'FROM') and \
            statement.tokens[5].match(sqlparse.tokens.Whitespace,' ',regex=True) and \
            (
                statement.tokens[6].match(sqlparse.tokens.Wildcard, '*') or \
                type(statement.tokens[6]) == sqlparse.sql.Identifier or \
                type(statement.tokens[6]) == sqlparse.sql.IdentifierList
            )

        # TODO do error raising here

        return is_valid

    def from_tables(self, stmt):
        """
        Pulls out the tables selected in select cols FROM TABLES
        """
        self.validate_from(stmt)
        return stmt.tokens[6]


    def validate_where(self, statement):
        """
        Validates the query to check if it is a FROM statement.
        This method checks only FROM tokens to see if they are a FROM keyword, whitespace, and an identifier.
        """
        isvalid = \
            statement.tokens[7].match(sqlparse.tokens.Whitespace,' ',regex=True) and \
            type(statement.tokens[8]) == sqlparse.sql.Where

        # TODO handle error raising here

        return is_valid

    def where_conds(self, stmt):
        """
        Pulls out the conditions selected in select cols from tables WHERE CONDS
        """
        self.validate_where(stmt)
        return '' # TODO


    def parse_select_from_where(self):

        # statements = sqlparse.parse(query)

        for stmt in self.statements:
            if (not self.validate(stmt)):
                raise RuntimeError('Query statement is invalid: ' + stmt)

            cols = self.select_cols(stmt) #TODO typecheck
            tbls = self.from_tables(stmt) #TODO typecheck
            # cond = self.where_conds(stmt) # TODO Where conditions


            return QueryFacade.query(cols, tbls, "")


        #return sqlparse.format(query, reindent=True, keyword_case='upper')


        """
        Parses the sql query stored in the parser

        # TODO handle subqueries
        """
