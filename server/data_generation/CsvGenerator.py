import datetime
import random

class CsvGenerator:
    def __init__(self, filename):
        self.filename = filename

    def generate(self, rows=10, col_generators={"A": lambda i: 'abcdefghijklmnopqrstuvwxyz'[i % 26]}):
        self.m = rows
        self.n = len(col_generators)

        with open(self.filename, 'w+') as f:
            # Write the column names to the csv file
            f.write(','.join([c for c in col_generators])+'\n')

            # Generate a row based on an index seed and write it to file
            for i in range(self.m):
                row = []
                for col in col_generators:
                    row.append(str(col_generators[col](i)))
                f.write(','.join(row) + '\n')


if __name__ == "__main__":
    dg = CsvGenerator('../../data/out.csv')
    now = datetime.datetime.now()

    # Generate 289 random attributes
    alphabet = 'abcedfjhijklmnopqrstuvwxyz'
    generators = {}
    for i in range(18):
        for j in range(18):
            generators[alphabet[i]+alphabet[j]] = lambda i: random.randint(1,1001)

    # generators = {
    #     "A": lambda i: 'abcdefghijklmnopqrstuvwxyz'[i % 26],
    #     "B": lambda i: i,
    #     "C": lambda i: i % 2 == 0,
    #     "D": lambda i: now.replace(hour=i)
    # }

    dg.generate(rows = 1000000, col_generators=generators)
