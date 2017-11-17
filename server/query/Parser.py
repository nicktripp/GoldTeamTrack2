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

    def consume_whitespace(self, stmt, idx):
        """
        Returns the next idx that isn't whitespace without performing any actions.
        """
        i = idx
        while (i < len(stmt.tokens) and stmt.tokens[i].match(sqlparse.tokens.Whitespace,' ',regex=True)):
            i += 1
        return i


    def validate_identifier(self, stmt, idx):
        """
        Tests the token at the given index to see if it is a valid Identifier or list of identifers
        """
        return                                                          \
            stmt.tokens[idx].match(sqlparse.tokens.Wildcard, '*') or    \
            type(stmt.tokens[idx]) == sqlparse.sql.Identifier or        \
            type(stmt.tokens[idx]) == sqlparse.sql.IdentifierList

    def consume_select(self, stmt, idx):

        if (stmt.get_type() != u'SELECT'):
            pass # TODO THROW ERROR

        # Eliminate leading whitespace
        idx = self.consume_whitespace(stmt, idx)

        # Sanity check that SELECT is next token
        if(not stmt.tokens[idx].match(sqlparse.tokens.DML, "SELECT")):
            pass # TODO THROW ERROR
        idx += 1 # Advance cursor

        # Eliminate whitespace trailing after SELECT
        idx = self.consume_whitespace(stmt, idx)

        # Next Token is our set of selected cols
        if (not self.validate_identifier(stmt, idx)):
            pass # TODO THROW ERROR

        self.cols = stmt.tokens[idx]
        return idx+1



    def consume_from(self, stmt, idx):
        """
        # TODO: DOES NOT HANDLE JOINS!!
        """

        # Eliminate leading whitespace
        idx = self.consume_whitespace(stmt, idx)

        # Sanity check that FROM is next token
        if(not stmt.tokens[idx].match(sqlparse.tokens.Keyword,'FROM')):
            pass # TODO THROW ERROR
        idx += 1 # Advance cursor

        # Eliminate whitespace trailing after FROM
        idx = self.consume_whitespace(stmt, idx)

        # Next Token is our set of selected tables
        if (not self.validate_identifier(stmt, idx)):
            pass # TODO THROW ERROR

        self.tbls = stmt.tokens[idx]
        return idx+1


    def consume_where(self, stmt, idx):
        """
        TODO WHERE
        """

        # Eliminate leading whitespace
        idx = self.consume_whitespace(stmt, idx)

        # Sanity check that WHERE is next token
        if(not type(stmt.tokens[idx]) == sqlparse.sql.Where):
            pass # TODO THROW ERROR

        self.where = stmt.tokens[idx]
        self.parse_where(self.where, 0)

        return idx+1

    def parse_where(self, stmt, idx):
        """
        # TODO condition parsing
        # TODO Multiple conditions
        # TODO LIKE conditon
        """
        # Sanity check that WHERE is next token
        if(stmt.tokens[idx].match(sqlparse.tokens.Keyword,"WHERE")):
            pass # TODO THROW ERROR
        idx = idx + 1

        # Eliminate whitespace trailing after WHERE
        idx = self.consume_whitespace(stmt, idx)

        self.cond = stmt.tokens[idx]


    def parse_select_from_where(self):

        for stmt in self.statements:
            if (not self.validate(stmt)):
                raise RuntimeError('Query statement is invalid: ' + stmt)

            idx = 0
            idx = self.consume_select(stmt, idx)
            idx = self.consume_from(stmt, idx)
            idx = self.consume_where(stmt, idx)


            return QueryFacade.query(self.cols, self.tbls, self.cond)


        #return sqlparse.format(query, reindent=True, keyword_case='upper')


        """
        Parses the sql query stored in the parser

        # TODO handle subqueries
        """
