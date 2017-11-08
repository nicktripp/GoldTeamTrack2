import pandas as pd

class Table:
    """
    Table opens a CSV format file for reading as needed in order
    to support SELECT * FROM * WHERE * queries over tables as large
    as millions of rows and up to 300 attributes

    Attributes will be one of Integer, Real, Text, Date, or Boolean
    """

    def __init__(self, filename):
        """
        Initializes a Table with the filename corresponding to the location of
        a CSV format file
        :param filename: location of .csv file
        """
        self.filename = filename

        # Read attribute information from file
        t = pd.read_csv(self.filename, nrows=2)
