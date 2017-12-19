import datetime
import os
from dateutil.parser import parse as date_parse

from server.query.TableProjector import TableProjector


class Table:
    relative_path = "./data/%s.csv"

    def __init__(self, name, nickname=None):
        self._name = name
        self._nickname = nickname
        assert os.path.exists(self.filename), "The table (%s) must exist." % self.filename

        # Fill the column index
        with open(self.filename, 'r', encoding='utf8') as f:
            columns = f.readline()[:-1].split(',')
            self._column_index = {col: i for i, col in enumerate(columns)}

        # Check the types of the columns
        self._column_types = {}
        self._check_column_types()

    def __repr__(self):
        if self._nickname is None:
            return "Table " + self._name
        else:
            return "Table %s %s" % (self._name, self._nickname)

    def __eq__(self, other):
        if isinstance(other, Table):
            if self.nickname is not None and self.nickname != other.nickname:
                return False
            return self.name == other.name
        return False

    @property
    def name(self):
        return self._name

    @property
    def column_index(self):
        return self._column_index

    @property
    def nickname(self):
        return self._nickname

    @property
    def filename(self):
        return Table.relative_path % self.name

    def _check_column_types(self):
        with open(self.filename, encoding='utf8') as f:
            cols = f.readline()[:-1].split(',')
            i = 0
            p = f.tell()
            size = os.path.getsize(self.filename)
            while i < 3 and p < size:
                col_vals = TableProjector.read_col_vals_multiline(p, f)
                p = f.tell()
                for col, val in zip(cols, col_vals):
                    if val == "":
                        # Wait for the value to be filled out
                        continue
                    if col not in self._column_types:
                        parsed = self.parse_value(val)
                        self._column_types[col] = type(parsed)
                    else:
                        assert self._column_types[col] != self.parse_value(val), "Values in a column must " \
                                                                               "have the same type. "
                # Read several rows past once every  type has been seen once
                if len(self._column_types) == len(self._column_index):
                    i += 1

    def parse_value_for_column(self, value, column_name):
        t = self._column_types[column_name]
        if t is int:
            if value == "":
                return 0
            return int(value)
        elif t is float:
            if value == "":
                return 0.0
            return float(value)
        elif t is bool:
            if value == "":
                return False
            if value == 'True' or value == 'true':
                return True
            elif value == 'False' or value == 'false':
                return False
        elif t is datetime.datetime:
            return date_parse(value)
        elif t is str:
            return value
        else:
            return None

        assert False, "All values should have been parsed. %s was not parsed as a %s for %s" % (value, t, column_name)

    def parse_value(self, val):
        try:
            return int(val)
        except ValueError:
            pass

        try:
            return float(val)
        except ValueError:
            pass

        if val == 'True' or val == 'true':
            return True
        elif val == 'False' or val == 'false':
            return False

        try:
            return date_parse(val)
        except ValueError:
            pass

        # Treat the right as TEXT
        return val
