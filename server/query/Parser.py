import sqlparse

"""
Parses an SQL query
"""

def validate(str):
    #TODO
    return True
    """
    Validates the string to check if it is valid SQL.
    Returns an error if it is invalid SQL.
    """


def parse(query):
    statements = sqlparse.parse(query)

    for stmt in statements:
        if (not validate(stmt)):
            raise RuntimeError('Query statement is invalid: ' + stmt)

    return sqlparse.format(query, reindent=True, keyword_case='upper')


    """
    Parses an sql query.
    """
