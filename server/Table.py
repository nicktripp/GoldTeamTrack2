import pandas as pd
import numpy as np
from dateutil.parser import parse
from multiprocessing import Pool
from itertools import repeat
from server.query.Condition import Condition


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
        # Get basic info about table
        self.filename = filename
        self.cols = {}
        self.get_column_types()

        # Start a worker pool
        self.num_partitions = 10
        self.num_cores = 4
        self.workers = Pool(self.num_cores)

    def where(self, conditions):
        """
        Opens the CSV file for reading in chunks
        Splits the chunks for multithreading
        Maps the condition functions onto the chunks
        Reduces the map results into a single dataframe and returns it
        :param conditions: predicates that must be satisfied for a row to be present in the output dataframe
        :return: dataframe of all of the rows that satisfy the conditions
        """
        dfs = []
        for df in pd.read_csv(self.filename, chunksize=100):
            mask = np.ones(len(df.index), dtype=bool)
            for condition in conditions:
                # Look up the values' indices needed in the condition
                values = df[[condition.get_first(), condition.get_second()]]
                values_split = np.array_split(values, self.num_partitions)

                # Pass the necessary columns to the workers and maintain a mask from the map
                condition_mask = pd.concat(self.workers.map(lambda row: condition.apply(row[0], row[1]), values_split))
                mask = np.logical_and(mask, condition_mask)
            dfs.append(df.loc[mask, :])
        return pd.concat(dfs)

    @staticmethod
    def is_date(string):
        try:
            parse(string)
            return True
        except ValueError:
            return False

    def get_column_types(self):
        t = pd.read_csv(self.filename, nrows=2)
        for i, c in enumerate(t):
            dtype = t[c].dtype
            if dtype == int:
                self.cols[c] = i, "Integer"
            elif dtype == bool:
                self.cols[c] = i, "Boolean"
            elif Table.is_date(t[c][0]):
                self.cols[c] = i, "Date"
            else:
                self.cols[c] = i, "Text"

if __name__ == "__main__":
    t = Table('../data/out.csv')
    c = Condition('aa', 'ab')

    print(t.where([c]))
