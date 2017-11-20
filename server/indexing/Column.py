from enum import Enum
from dateutil.parser import parse

class ColumnType(Enum):
    """
    These are the types of columns we will support
    """
    INTEGER = 0
    REAL = 1
    TEXT = 2
    DATE = 3
    BOOLEAN = 4
    UNKNOWN = 5

class Column:
    """
    Represents a file in a csv
    """

    def __init__(self, name, type=ColumnType.UNKNOWN):
        self.name = name
        self.type = type

    def __repr__(self):
        return "Column %s[%s]" % (self.name, self.type)

    @staticmethod
    def get_from_headers(headers_line):
        columns = headers_line[:-1].split(',')
        return [Column(name) for name in columns]

    def get_type(value):
        # Try to parse as int, float, boolean, then date
        # Fallback to text
        try:
            parse(value)
            return ColumnType.INTEGER
        except ValueError:
            try:
                float(value)
                return ColumnType.REAL
            except ValueError:
                try:
                    int(value)
                    return ColumnType.DATE
                except ValueError:
                    if value in {'True', 'False', 'true', 'false'}:
                        return ColumnType.BOOLEAN
                    else:
                        return ColumnType.TEXT

        return ColumnType.UNKNOWN
