import dask.dataframe as dd
import shutil

import os


class Organizer:
    """
    Reads a CSV file from filename
    Writes the CSV file by column as CSV files in the output_directory
    """

    def __init__(self, input_csv, output_directory="../../data/tmp/"):
        self.input_csv = input_csv
        self.output_directory = output_directory

    def colonize(self):
        # Reset the output directory
        shutil.rmtree(self.output_directory)
        os.makedirs(self.output_directory)

        # Use dask to read large CSV files
        df = dd.read_csv(self.input_csv)
        for col in df:
            df[col].to_csv(self.output_directory+col)

if __name__ == "__main__":
    o = Organizer()
