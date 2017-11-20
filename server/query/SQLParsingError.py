

class SQLParsingError(Exception):
    """
    Holds information when encountering an error parsing an SQL query.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message

    def __str__(self):
        return(self.message + ": '" + self.expression + "'")