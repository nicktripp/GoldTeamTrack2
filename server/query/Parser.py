import sqlparse

from server.query.Comparison import Comparison
from server.query.SQLParsingError import SQLParsingError

LOG = True

class Parser:
    """
    Parses an SQL query, token by token using the sqlparse library.

    Internal representation of the query holds tokens at indicies.
    Most of the following functions "consume" a token or set of tokens
        by processing it and then advancing the "cursor" as appropriate.

    TODO:
        * Handle adv. keywords, like 'DISTINCT'
        * Handle renaming columns
        * Handle joins
        * Handle complex (Parentheses) boolean logic
        * Further parse the conditions for ease of use.
        * Handle more than one statement at once, or throw an error
    """

    def __init__(self, query_string):
        """
        Loads a new query into the parser.
        """

        self.query_string = query_string
        self.statements = sqlparse.parse(query_string)
        self.cols  = []     # Set of selected column names
        self.tbls  = []     # Set of selected table names.  Format is a 2D list,  [[],[]] where each sublist is CROSSED and each sublist is JOINED together
        self.conds = []     # Set of conditions.  Format is a list of lists of conditions.
                            # TODO: IMPLEMENT THIS\/\/
                            #   Each sublist is a set of conditions joined by ANDS, and each sublist is joined by an OR.
                            #   [
                            #       [ <SomeCondition>, <ANDAnotherCondition> ] (Conditions AND)
                            #       [ <ORAnotherConditionSet> ]
                            #   ]
                            # TODO: IMPLEMENT THIS^^

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
        Advances the cursor by returning the index of the next non-whitespace
            token without performing any actions.

        @returns the specified index.
        """
        i = idx
        while (i < len(stmt.tokens) and stmt.tokens[i].match(sqlparse.tokens.Whitespace,' ',regex=True)):
            i += 1
        return i

    def validate_identifier(self, stmt, idx):
        """
        Tests the token at the given index to see if it is either
            a valid Identifier or
            a list of identifers

        @returns a boolean
        """
        return                                                          \
            stmt.tokens[idx].match(sqlparse.tokens.Wildcard, '*') or    \
            type(stmt.tokens[idx]) == sqlparse.sql.Identifier or        \
            type(stmt.tokens[idx]) == sqlparse.sql.IdentifierList

    def consume_select(self, stmt, idx):
        """
        Consumes a token phrase in the form of SELECT [columns].

        Stores the columns specified in the self.cols list.
        Advances the cursor to the index after this token phrase.

        TODO:
            * Handle keywords such as 'DISTINCT'
            * Handle renaming columns

        @returns the index of the token after this token phrase
        """
        if (stmt.get_type() != u'SELECT'):
            raise SQLParsingError( sqlparse.format(stmt.value, keyword_case='upper'), "Not a SELECT statement")

        # Eliminate leading whitespace
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt,idx)

        # Sanity check that SELECT is next token
        if(not stmt.tokens[idx].match(sqlparse.tokens.DML, "SELECT")):
            raise SQLParsingError( sqlparse.format(stmt.value, keyword_case='upper'), "Unrecognized SELECT format")
        idx += 1 # Advance cursor

        # Eliminate whitespace trailing after SELECT
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt,idx)

        # Next Token is our set of selected cols
        if (not self.validate_identifier(stmt, idx)):
            raise SQLParsingError( sqlparse.format(stmt.value, keyword_case='upper'), "Unrecognized SELECT format")

        self.cols = self.token_to_list(stmt.tokens[idx])

        return idx+1

    def token_to_list(self, token):
        """
        Converts a Token or TokenList to a list of strings.

        Can only handle Identifiers, IdentifierLists, and Wildcards.

        @returns a list of the string value of tokens
        """
        if(type(token) == sqlparse.sql.Identifier or token.ttype == sqlparse.tokens.Wildcard):
            return [token.value]
        elif(type(token) == sqlparse.sql.IdentifierList):
            # Convert list recursively
            return self.tokenlist_to_list(token)
        else:
            raise SQLParsingError( token.value, "Invalid Identifier")

    def tokenlist_to_list(self, tokenlist):
        """
        Helper function that converts a TokenList into a list of tokens.

        @returns a list of the string value of tokens.
        """
        l = []
        for sub_token in tokenlist.tokens:
            if(type(sub_token) == sqlparse.sql.Identifier or sub_token.ttype == sqlparse.tokens.Wildcard):
                l.append(sub_token.value)

        return l

    def consume_from(self, stmt, idx):
        """
        Consumes a token phrase in the form of FROM [tables].

        Stores the tables specified in the self.tbls list.
        Advances the cursor to the index after this token phrase.
        Renaming tables is supported:
            Consuming 'FROM movies M, oscars O' stores ['movies M','oscars O']
            in self.tbls

        TODO:
            * Handle joins

        @returns the index of the token after this token phrase
        """
        self.check_index(stmt,idx)

        # Eliminate leading whitespace
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt,idx)

        self.print_curr_token(stmt, idx) # LOG

        # Sanity check that FROM is next token
        if(not stmt.tokens[idx].match(sqlparse.tokens.Keyword,'FROM')):
            raise SQLParsingError(stmt.tokens[idx].value, "Expected 'FROM' keyword")
        idx += 1 # Advance cursor

        # Eliminate whitespace trailing after FROM
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt,idx)

        self.print_curr_token(stmt, idx) # LOG

        # Next Token is our set of selected tables
        if (not self.validate_identifier(stmt, idx)):
            raise SQLParsingError( stmt.tokens[idx].value, "Invalid Identifier")


        self.tbls = self.token_to_list(stmt.tokens[idx])

        return idx+1

    def consume_where(self, stmt, idx):
        """
        Consumes a WHERE token.
        Most of the meat of this function is in parse_where()

        Stores the conditions specified in the self.conds list.
        Advances the cursor to the index after this token.

        @returns the index of the token after the WHERE token
        """

        # Eliminate leading whitespace
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt,idx)

        # Sanity check that WHERE is next token
        if(not type(stmt.tokens[idx]) == sqlparse.sql.Where):
            raise SQLParsingError( sqlparse.format(stmt.value, keyword_case='upper'), "Expected 'WHERE' keyword")

        self.where = stmt.tokens[idx]
        self.parse_where(self.where, 0)

        return idx+1

    def parse_where(self, stmt, idx):
        """
        Consumes a token phrase in the form of
            WHERE <Comparison> (<AND|OR> ...) or
            WHERE <Identifier> LIKE <Identifier> (<AND|OR> ...)

        Stores the conditions specified in the self.conds list.
        Advances the cursor to the index after this token phrase.

        Any included parentheses will not be processed and will result in an error.

        TODO:
            * Handle complex (Parentheses) boolean logic

        @returns the index of the token after this token phrase
        """

        self.print_curr_token(stmt, idx) # LOG

        # Sanity check that WHERE is next token
        if(not stmt.tokens[idx].match(sqlparse.tokens.Keyword,"WHERE")):
            raise SQLParsingError( sqlparse.format(stmt.value, keyword_case='upper'), "Expected 'WHERE' keyword")
        idx = idx + 1

        self.conds = []

        expecting = True # Are we expecting more tokens as input?

        while(idx < len(stmt.tokens)):
            idx = self.consume_condition(stmt, idx)
            if(idx >= len(stmt.tokens)):
                expecting = False
                break
            idx = self.consume_whitespace(stmt, idx)
            if(idx >= len(stmt.tokens)):
                expecting = False
                break
            idx = self.consume_logic(stmt, idx)
            expecting = True

        if(expecting):
            raise SQLParsingError( sqlparse.format(stmt.value, keyword_case='upper'), "Unexpected end of query")

        return idx+1

    def consume_condition(self, stmt, idx):
        """
        Consumes the part of a WHERE token phrase after the WHERE.
        Thus, in consumes token phrases in the form of
            (<Comparison>) or
            (<Identifier> LIKE <Identifier>)

        Appends the condition encountered to the list stored in self.conds, where
            (<Comparison>) -> [<Comparison>]
            (<Identifier> LIKE <Identifier>) -> [Identifier.value, 'LIKE', Identifier.value]


        @returns the index of the token after this token phrase
        """
        # Eliminate whitespace before COMPARISON
        idx = self.consume_whitespace(stmt, idx)

        self.print_curr_token(stmt, idx) # LOG


        # check that COMPARISON is next token
        if(type(stmt.tokens[idx]) == sqlparse.sql.Comparison):
            self.conds.append(stmt.tokens[idx]) # Add comparison to list

        elif (type(stmt.tokens[idx]) == sqlparse.sql.Identifier):
            # Handling the LIKE case:
            self.conds.append([stmt.tokens[idx].value])

            idx += 1
            self.check_index(stmt,idx)



            # Eliminate whitespace before LIKE
            idx = self.consume_whitespace(stmt, idx)
            self.check_index(stmt,idx)

            self.print_curr_token(stmt, idx) # LOG

            if(not stmt.tokens[idx].match(sqlparse.tokens.Keyword,'LIKE')):
                raise SQLParsingError( sqlparse.format(stmt.value, keyword_case='upper'), "Expected 'LIKE' keyword")
            self.conds[-1].append("LIKE")
            idx += 1

            self.check_index(stmt,idx)

            idx = self.consume_whitespace(stmt, idx)

            self.check_index(stmt,idx)

            if (not self.validate_identifier(stmt, idx)):
                raise SQLParsingError( stmt.tokens[idx].value, "Invalid Identifier")

            self.conds[-1].append(stmt.tokens[idx].value)
        else:
            raise SQLParsingError( stmt.tokens[idx].value, "Invalid Identifier")


        return idx+1

    def consume_logic(self, stmt, idx):
        """
        Consumes a boolean logic phrase in the style of (AND|OR).
        Appends the boolean operator to self.conds, separating conditons.

        @returns the index of the token after this token phrase
        """
        if(stmt.tokens[idx].match(sqlparse.tokens.Keyword,"AND")):
            self.conds.append("AND")
        elif(stmt.tokens[idx].match(sqlparse.tokens.Keyword,"OR")):
            self.conds.append("OR")
        else:
            raise SQLParsingError( stmt.tokens[idx].value, "Not a boolean operator")


        return idx + 1

    def parse_select_from_where(self):
        """
        Parses a standard SELECT-FROM-WHERE statement.

        Currently, it only processes a single statement (the first one) and ignores
            all others following, even though sqlparser supports multiple
            statements separated by semicolons.

        TODO:
            * Handle more than one statement at once, or throw an error

        @returns the results of the query after passing it on to QueryFacade.
        """

        for stmt in self.statements:
            if (not self.validate(stmt)):
                raise SQLParsingError( sqlparse.format(stmt.value, keyword_case='upper'), 'Unrecognized SQL')

            idx = 0
            idx = self.consume_select(stmt, idx)
            idx = self.consume_from(stmt, idx)

            if(idx < len(stmt.tokens)):
                idx = self.consume_where(stmt, idx)
            else:
                self.conds = []

            # Convert sqlparse Comparison to our Comparison
            self.conds = [Comparison(c) for c in self.conds]

            return self.cols, self.tbls, self.conds
            # return QueryFacade.query(self.cols, self.tbls, self.conds)

    def check_index(self, stmt, idx):
        """
        Checks to see if the cursor is out of range based on its given index.
        Raises an error if so.
        """
        if(idx >= len(stmt.tokens)):
            raise SQLParsingError( sqlparse.format(stmt.value, keyword_case='upper'), "Unexpected end of query")


    def print_curr_token(self, stmt, idx):
        """
        If logging is turned on, prints the value of the token at the specified
            index.
        """
        if LOG:
            print("Current token: '" + stmt.tokens[idx].value + "'")