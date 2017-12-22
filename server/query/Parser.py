import sqlparse

from server.query.Column import Column
from server.query.Comparison import Comparison
from server.query.SQLParsingError import SQLParsingError
from server.query.Join import *
from server.query.Table import Table

LOG = False


class Parser:
    """
    Parses an SQL query, token by token using the sqlparse library.

    Internal representation of the query holds tokens at indicies.
    Most of the following functions "consume" a token or set of tokens
        by processing it and then advancing the "cursor" as appropriate.

    TODO:
        * Handle renaming columns
        * Handle more than one statement at once, or throw an error
    """

    def __init__(self, query_string):
        """
        Loads a new query into the parser.
        """

        self.query_string = query_string
        self.statements = sqlparse.parse(query_string)
        self.cols = []  # Set of selected column names
        self.tbls = []  # Set of selected table names. Returned as a list of Table objects
        self.conds = []  # Set of conditions.  Format to be changed.
        self.join_conds = None  # Set of Join conditions, if they are there.
        self.is_distinct = False  # Boolean describing if the keyword DISTINCT is used.

    @staticmethod
    def validate(stmt):
        """
        Performs very basic validation the query to check if it is valid SQL.
        Returns an FALSE if it is invalid SQL.
        """

        return stmt.get_type() != u'UNKNOWN'  # Check if query begins with a valid DDL or DML keyword
        # More robust validation handled below

    def consume_whitespace(self, stmt, idx):
        """
        Advances the cursor by returning the index of the next non-whitespace
            token without performing any actions.

        @returns the specified index.
        """
        i = idx
        while i < len(stmt.tokens) and stmt.tokens[i].match(sqlparse.tokens.Whitespace, ' ', regex=True):
            i += 1
        return i


    def validate_identifier(self, stmt, idx):
        """
        Tests the token at the given index to see if it is either
            a valid Identifier or
            a list of identifers

        @returns a boolean
        """
        if(stmt.tokens[idx].is_group):
            if len(stmt.tokens[idx].tokens) == 3:
                return \
                    type(stmt.tokens[idx].tokens[2]) == sqlparse.sql.Identifier

            if len(stmt.tokens[idx].tokens) == 5:
                return \
                    type(stmt.tokens[idx].tokens[0]) == sqlparse.sql.Identifier

        return \
            stmt.tokens[idx].match(sqlparse.tokens.Wildcard, '*') or \
            type(stmt.tokens[idx]) == sqlparse.sql.Identifier or \
            type(stmt.tokens[idx]) == sqlparse.sql.IdentifierList

    def consume_select(self, stmt, idx):
        """
        Consumes a token phrase in the form of SELECT [columns].

        Stores the columns specified in the self.cols list.
        Advances the cursor to the index after this token phrase.

        TODO:
            * Handle renaming columns

        @returns the index of the token after this token phrase
        """
        if (stmt.get_type() != u'SELECT'):
            raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), "Not a SELECT statement")

        # Eliminate leading whitespace
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt, idx)

        # Sanity check that SELECT is next token
        if (not stmt.tokens[idx].match(sqlparse.tokens.DML, "SELECT")):
            raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), "Unrecognized SELECT format")
        idx += 1  # Advance cursor

        # Eliminate whitespace trailing after SELECT
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt, idx)

        # Check to see if DISTINCT is present.
        if (self.is_keyword_remaining(stmt, idx, "DISTINCT")):
            if (not stmt.tokens[idx].match(sqlparse.tokens.Keyword, "DISTINCT")):
                raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'),
                                      "Unrecognized SELECT DISTINCT format")
            self.is_distinct = True
            idx += 1  # Advance cursor
            # Eliminate whitespace trailing after DISTINCT
            idx = self.consume_whitespace(stmt, idx)
            self.check_index(stmt, idx)

        # Next Token is our set of selected cols
        if (not self.validate_identifier(stmt, idx)):
            raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), "Unrecognized SELECT format")


        self.cols = self.token_to_list(stmt.tokens[idx])

        return idx + 1

    def token_to_list(self, token):
        """
        Converts a Token or TokenList to a list of strings.

        Can only handle Identifiers, IdentifierLists, and Wildcards.

        @returns a list of the string value of tokens
        """
        if(token.is_group & (len(token.tokens) == 5)):
            return [(token.tokens[0],token.tokens[2], token.tokens[4])]
        if (type(token) == sqlparse.sql.Identifier or token.ttype == sqlparse.tokens.Wildcard):
            return [(token.value, None, None)]
        elif (type(token) == sqlparse.sql.IdentifierList):
            # Convert list recursively
            return self.tokenlist_to_list(token)
        else:
            raise SQLParsingError(token.value, "Invalid Identifier")

    def tokenlist_to_list(self, tokenlist):
        """
        Helper function that converts a TokenList into a list of tokens.

        @returns a list of the string value of tokens.
        """
        l = []
        for sub_token in tokenlist.tokens:
            if (type(sub_token) == sqlparse.sql.Identifier or sub_token.ttype == sqlparse.tokens.Wildcard):
                l.append((sub_token.value,None, None))

        return l

    def str_to_Table(self, tbl_str):
        tbl_list = tbl_str.split(" ")

        if (len(tbl_list) != 1 and len(tbl_list) != 2):
            raise ValueError("Unrecognized string-to-Table format")

        return Table(*tbl_list)

    def is_keyword_remaining(self, stmt, idx, keyword):
        """
        Checks the query after the specified index to see if there is a KEYWORD statement remaining.
        """
        while idx < len(stmt.tokens):
            if stmt.tokens[idx].match(sqlparse.tokens.Keyword, keyword):
                return True
            idx += 1
        return False

    def consume_join(self, stmt, idx):
        """
        Consumes a JOIN phrase in the form of JOIN [tables]
        Advances the cursor to the index after this token phrase.

        :return: the index of the token after this token phrase.
        """
        # Eliminate whitespace before potential JOIN
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt, idx)

        # Check if JOIN is next token
        if not stmt.tokens[idx].match(sqlparse.tokens.Keyword, "JOIN"):
            raise SQLParsingError(stmt.tokens[idx].value, "Expected 'JOIN' keyword")

        idx += 1  # Advance cursor

        self.check_index(stmt, idx)

        # Eliminate whitespace trailing after JOIN, before right-hand table
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt, idx)

        # Next Token is our right-hand set of selected tables
        if not self.validate_identifier(stmt, idx):
            raise SQLParsingError(stmt.tokens[idx].value, "Invalid Identifier")

        tbls = self.token_to_list(stmt.tokens[idx])
        for tbl in tbls:
            self.tbls.append(self.str_to_Table(tbl))

        return idx + 1

    def consume_on(self, stmt, idx, has_joined):
        """
        Consumes an ON phrase in the form of ON (<conditions>)
        Advances the cursor to the index after this token phrase.

        NOTE: Puts the conditions into self.conds, to be ANDed to later conditions.
        """
        # Eliminate whitespace before potential ON keyword
        idx = self.consume_whitespace(stmt, idx)

        # Check if ON is next token
        if not stmt.tokens[idx].match(sqlparse.tokens.Keyword, "ON"):
            raise SQLParsingError(stmt.tokens[idx].value, "Expected 'ON' keyword")

        if not has_joined:
            raise SQLParsingError(stmt.tokens[idx].value, "Unexpected 'ON' keyword")

        idx += 1  # advance cursor
        self.check_index(stmt, idx)

        # Eliminate whitespace trailing after ON
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt, idx)

        # Puts the conditional joins conditions into the list of overall conditions.
        idx, conds = self.consume_condition(stmt, idx)
        self.join_conds = conds

        # Enforce that these conditions are all equality conditions
        self.validate_join_conditions(stmt, self.join_conds)

        return idx

    def validate_join_conditions(self, stmt, conds):
        if not conds:
            return

        for item in conds[1]:
            if type(item[1]) is list:
                self.validate_join_conditions(stmt, item)
            else:
                assert type(item[1]) is Comparison
                if item[1].operator != '=':
                    raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'),
                                          "JOIN conditions must be equality comparisons!")

    def consume_from(self, stmt, idx):
        """
        Consumes a token phrase in the form of FROM [tables].

        Stores the tables specified in self.tbls as a list of Tables
        Advances the cursor to the index after this token phrase.
        Renaming tables is supported:
            Consuming 'FROM movies M, oscars O' stores ['movies M','oscars O']
            in self.tbls

        @returns the index of the token after this token phrase
        """
        self.check_index(stmt, idx)

        # Eliminate leading whitespace
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt, idx)

        self.print_curr_token(stmt, idx)  # LOG

        # Sanity check that FROM is next token
        if not stmt.tokens[idx].match(sqlparse.tokens.Keyword, 'FROM'):
            raise SQLParsingError(stmt.tokens[idx].value, "Expected 'FROM' keyword")
        idx += 1  # Advance cursor

        # Eliminate whitespace trailing after FROM
        idx = self.consume_whitespace(stmt, idx)
        self.check_index(stmt, idx)

        self.print_curr_token(stmt, idx)  # LOG

        # Next Token is our set of selected tables
        if not self.validate_identifier(stmt, idx):
            raise SQLParsingError(stmt.tokens[idx].value, "Invalid Identifier")

        tbls = self.token_to_list(stmt.tokens[idx])
        for tbl in tbls:
            self.tbls.append(self.str_to_Table(tbl[0]))

        idx += 1

        # If nothing more to process, graceful exit
        if idx >= len(stmt.tokens):
            return idx

        # Process all JOIN statements
        has_join = False
        while self.is_keyword_remaining(stmt, idx, "JOIN"):
            has_join = True
            idx = self.consume_join(stmt, idx)

        # If nothing more to process, graceful exit
        if idx >= len(stmt.tokens):
            return idx

        # Process potential ON statement:
        if self.is_keyword_remaining(stmt, idx, "ON"):
            idx = self.consume_on(stmt, idx, has_join)

        return idx

    def list_to_Join(self, table_list):
        if not table_list:
            raise ValueError("List must have at least one element.")
        elif len(table_list) == 1:
            return SingletonJoin(table_list[0])
        else:
            return Join(SingletonJoin(table_list[0]), JoinType.CROSS, self.list_to_Join(table_list[1:]))

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
        self.check_index(stmt, idx)

        # Sanity check that WHERE is next token
        if not type(stmt.tokens[idx]) == sqlparse.sql.Where:
            raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), "Expected 'WHERE' keyword")

        self.where = stmt.tokens[idx]
        self.parse_where(self.where, 0)

        return idx + 1

    def parse_where(self, stmt, idx):
        """
        Consumes a token phrase in the form of
            WHERE <Comparison> (<AND|OR> ...) or
            WHERE <Identifier> LIKE <Identifier> (<AND|OR> ...)

        Stores the conditions specified in the self.conds list.
        Advances the cursor to the index after this token phrase.

        Any included parentheses will not be processed and will result in an error.

        @returns the index of the token after this token phrase
        """

        self.print_curr_token(stmt, idx)  # LOG

        # Sanity check that WHERE is next token
        if not stmt.tokens[idx].match(sqlparse.tokens.Keyword, "WHERE"):
            raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), "Expected 'WHERE' keyword")
        idx += 1
        self.check_index(stmt, idx)

        idx, conds = self.consume_multiple_conditions(stmt, idx)
        self.conds = conds

        return idx + 1

    def consume_multiple_conditions(self, stmt, idx):
        conds = (False, [(False, [])])
        expecting = True
        while idx < len(stmt.tokens):
            idx, condition = self.consume_condition(stmt, idx)
            conds[1][-1][1].append(condition)
            if idx >= len(stmt.tokens):
                expecting = False
                break
            idx = self.consume_whitespace(stmt, idx)
            if idx >= len(stmt.tokens):
                expecting = False
                break
            idx, conds = self.consume_logic(stmt, idx, conds)

        if expecting:
            raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), "Unexpected end of query")

        return idx, conds

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

        self.print_curr_token(stmt, idx)  # LOG

        # check that COMPARISON is next token
        if type(stmt.tokens[idx]) == sqlparse.sql.Comparison:
            return idx + 1, (False, Comparison(stmt.tokens[idx]))
        elif type(stmt.tokens[idx]) == sqlparse.sql.Identifier:
            # Handling the LIKE case:
            column = stmt.tokens[idx].value
            idx += 1
            self.check_index(stmt, idx)

            # Eliminate whitespace before LIKE
            idx = self.consume_whitespace(stmt, idx)
            self.check_index(stmt, idx)

            self.print_curr_token(stmt, idx)  # LOG

            if not stmt.tokens[idx].match(sqlparse.tokens.Keyword, 'LIKE'):
                raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), "Expected 'LIKE' keyword")
            idx += 1

            self.check_index(stmt, idx)

            idx = self.consume_whitespace(stmt, idx)

            self.check_index(stmt, idx)

            if not self.validate_identifier(stmt, idx):
                raise SQLParsingError(stmt.tokens[idx].value, "Invalid Identifier")

            pattern = stmt.tokens[idx].value
            return idx + 1, (False, Comparison.from_like(column, pattern))
        elif type(stmt.tokens[idx]) == sqlparse.sql.Parenthesis:
            # Handle nested conditions in parenthesis
            parenthesis = stmt.tokens[idx]
            idx += 1
            parenthesis.tokens = parenthesis.tokens[1:-1]
            _, conds = self.consume_multiple_conditions(parenthesis, 0)
            return idx, conds
        elif stmt.tokens[idx].ttype == sqlparse.tokens.Keyword and stmt.tokens[idx].match(sqlparse.tokens.Keyword,
                                                                                          'NOT'):
            # Handle NOT case
            idx += 1
            self.check_index(stmt, idx)
            idx = self.consume_whitespace(stmt, idx)

            # Require Parenthesis
            idx, cond_tuple = self.consume_condition(stmt, idx)
            return idx, (True, cond_tuple[1])
        else:
            raise SQLParsingError(stmt.tokens[idx].value, "Invalid Identifier")

        return idx + 1, None

    def consume_logic(self, stmt, idx, conds):
        """
        Consumes a boolean logic phrase in the style of (AND|OR).
        Appends the boolean operator to self.conds, separating conditons.

        @returns the index of the token after this token phrase
        """
        if len(conds) == 0:
            conds[1].append((False, []))
        if stmt.tokens[idx].match(sqlparse.tokens.Keyword, "AND"):
            # Continue appending to most recent logic_group
            pass
        elif stmt.tokens[idx].match(sqlparse.tokens.Keyword, "OR"):
            # Start a new logic group for new AND conditions
            conds[1].append((False, []))
        else:
            raise SQLParsingError(stmt.tokens[idx].value, "Not a boolean operator")

        return idx + 1, conds

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
            if not self.validate(stmt):
                raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), 'Unrecognized SQL')

            idx = 0
            idx = self.consume_select(stmt, idx)
            idx = self.consume_from(stmt, idx)

            if idx < len(stmt.tokens):
                self.consume_where(stmt, idx)
                if self.join_conds is not None:
                    self.conds = (False, [(False,[self.join_conds, self.conds])])
            else:
                self.conds = []
                if self.join_conds is not None:
                    self.conds = self.join_conds

            # Convert columns from strings to Column instances
            self.convert_columns(stmt)

            return self.cols, self.tbls, self.conds, self.is_distinct

    def convert_columns(self, stmt):
        # Create dictionary to lookup tables for columns in select
        tbl_dict = {}
        for tbl in self.tbls:
            if tbl.nickname is not None:
                tbl_dict[tbl.nickname] = tbl
            else:
                tbl_dict[tbl.name] = tbl
        # Convert the column string values to Column instances
        requires_prefix = len(self.tbls) == 1
        for i in range(len(self.cols)):
            # THIS WILL BE A STRING OR A TOKEN
            col_str, op, number = self.cols[i]
            if col_str == '*':
                if len(self.tbls) == 1:
                    self.cols[i] = Column(self.tbls[0], col_str)
                else:
                    raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), "Ambiguous use of the * "
                                                                                             "selector.")
                continue
            if(type(col_str) != str):
                table_column = col_str.value.split('.')
            else:
                table_column = col_str.split('.')
            if len(table_column) < 2:
                if requires_prefix:
                    self.cols[i] = Column(self.tbls[0], col_str)
                else:
                    raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'),
                                          "Unexpected end of query")
            else:
                tbl, name = tbl_dict[table_column[0]], table_column[1]
                self.cols[i] = Column(tbl, name, op, number)

    def check_index(self, stmt, idx):
        """
        Checks to see if the cursor is out of range based on its given index.
        Raises an error if so.
        """
        if (idx >= len(stmt.tokens)):
            raise SQLParsingError(sqlparse.format(stmt.value, keyword_case='upper'), "Unexpected end of query")

    def print_curr_token(self, stmt, idx):
        """
        If logging is turned on, prints the value of the token at the specified
            index.
        """
        if LOG:
            print("Current token: '" + stmt.tokens[idx].value + "'")
