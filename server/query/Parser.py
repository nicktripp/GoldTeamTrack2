import sqlparse

from server.query.QueryFacade import QueryFacade

class Parser:
    """
    Parses an SQL query, token by token using the sqlparse library.
    Internal representation of the query holds
    """

    def __init__(self, query_string):
        """
        Loads a new query into the parser.
        """
        self.query_string = query_string
        self.statements = sqlparse.parse(query_string)
        self.cols  = []     # Set of selected column names
        self.tbls  = []     # Set of selected table names
        self.conds = []     # Set of conditions

    @staticmethod
    def validate(stmt):
        """
        Performs very basic validation the query to check if it is valid SQL.
        Returns an FALSE if it is invalid SQL.
        """

        return stmt.get_type() != u'UNKNOWN' # Check if query begins with a valid DDL or DML keyword
        # More robust validation handled below

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
        """
        Parses the SELECT COLS part of the query, starting at index idx.
        Stores the selected cols in self.cols as a list of strings.
        Returns the new cursor position after the SELECT part.

        # TODO Does not handle keywords like DISTINCT
        """
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

        self.cols = self.token_to_list(stmt.tokens[idx])

        return idx+1

    def token_to_list(self, token):
        """
        Converts a Token or TokenList to a list of strings
        """
        if(type(token) == sqlparse.sql.Identifier or token.ttype == sqlparse.tokens.Wildcard):
            return [token.value]
        elif(type(token) == sqlparse.sql.IdentifierList):
            # Convert list recursively
            return self.tokenlist_to_list(token)
        else:
            # TODO THROW ERROR! Unknown token attempted to convert
            return ["ERROR!"]

    def tokenlist_to_list(self, tokenlist):
        l = []
        for sub_token in tokenlist.tokens:
            if(type(sub_token) == sqlparse.sql.Identifier or sub_token.ttype == sqlparse.tokens.Wildcard):
                l.append(sub_token.value)

        return l

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

        self.tbls = self.token_to_list(stmt.tokens[idx])

        return idx+1


    def consume_where(self, stmt, idx):

        # Eliminate leading whitespace
        idx = self.consume_whitespace(stmt, idx)

        # Sanity check that WHERE is next token
        if(not type(stmt.tokens[idx]) == sqlparse.sql.Where):
            pass # TODO THROW ERROR

        self.where = stmt.tokens[idx]
        self.parse_where(self.where, 0)

        return idx+1

    def consume_condition(self, stmt, idx):
        """
        # TODO better IndexError handling -> silenced above
        """
        # Eliminate whitespace before COMPARISON
        idx = self.consume_whitespace(stmt, idx)

        # check that COMPARISON is next token
        if(type(stmt.tokens[idx]) == sqlparse.sql.Comparison):
            self.conds.append(stmt.tokens[idx])

            # Add comparison to list
        elif (type(stmt.tokens[idx]) == sqlparse.sql.Identifier):
            # Handling the LIKE case:
            self.conds.append([stmt.tokens[idx].value])

            idx += 1

            # Eliminate whitespace before LIKE
            idx = self.consume_whitespace(stmt, idx)

            if(not stmt.tokens[idx].match(sqlparse.tokens.Keyword,'LIKE')):
                print("ERROR!") # TODO Handle error
            idx += 1
            self.conds[-1].append("LIKE")

            idx = self.consume_whitespace(stmt, idx)

            if (type(stmt.tokens[idx]) != sqlparse.sql.Identifier):
                print("ERROR!") # TODO Handle error
            self.conds[-1].append(stmt.tokens[idx].value)
        else:
            print("Bad Where clause")
            # print("ERROR!") # TODO Handle error

        return idx+1

    def consume_logic(self, stmt, idx):
        """
        """
        if(stmt.tokens[idx].match(sqlparse.tokens.Keyword,"AND")):
            self.conds.append("AND")
        elif(stmt.tokens[idx].match(sqlparse.tokens.Keyword,"OR")):
            self.conds.append("OR")
        else:
            pass # TODO THROW ERROR

        return idx + 1


    def parse_where(self, stmt, idx):
        """
        # TODO Multiple conditions
        # TODO LIKE conditon
        # TODO DOES NOT SUPPORT PARENTHESIS
        """
        # Sanity check that WHERE is next token
        if(stmt.tokens[idx].match(sqlparse.tokens.Keyword,"WHERE")):
            pass # TODO THROW ERROR
        idx = idx + 1

        self.conds = []

        while(idx < len(stmt.tokens)):
            idx = self.consume_condition(stmt, idx)
            if(idx >= len(stmt.tokens)):
                break
            idx = self.consume_whitespace(stmt, idx)
            if(idx >= len(stmt.tokens)):
                break
            idx = self.consume_logic(stmt, idx)

        return idx+1


    def parse_select_from_where(self):

        for stmt in self.statements:
            if (not self.validate(stmt)):
                raise RuntimeError('Query statement is invalid: ' + stmt)

            idx = 0
            idx = self.consume_select(stmt, idx)
            idx = self.consume_from(stmt, idx)
            try:
                idx = self.consume_where(stmt, idx)
            except IndexError:
                print("No WHERE clause found")
                self.conds = []


            return QueryFacade.query(self.cols, self.tbls, self.conds)
            # TODO technically the sql parser supports multiple SQL queries
            #       separated by a ';'.  We only process the first one...

