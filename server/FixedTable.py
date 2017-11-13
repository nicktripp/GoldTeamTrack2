import os
import time
import pickle
from collections import defaultdict
import multiprocessing as mp


class FixedTable:
    """
    Normalizes the columns of the input csv to fixed widths
    Traverses rows and columns efficiently using seek and read
    """

    def __init__(self, input_csv, tmp_dir):
        """
        The input_csv is normalized and written to output_csv
        """
        self.input_csv = input_csv
        self.tmp_dir = tmp_dir
        self.input_size = os.path.getsize(input_csv)
        self.widths = self.get_widths()

    def get_widths(self):
        """
        Finds the maximum width for each column in the input_csv.
        """
        # Print the amount of time it takes to find or read the widths
        start_time = time.time()
        end_time = 0
        widths = defaultdict(int)

        # Load the widths from a tmp pickle file
        widths_pickle = self.tmp_dir + 'widths.pickle'
        if os.path.exists(widths_pickle):
            with open(widths_pickle, 'rb') as f:
                widths = pickle.load(f)
            end_time = time.time()
        else:
            # Read the widths off of the input csv
            widths = self.read_widths()

            # Done reading widths
            end_time = time.time()

            # Save the widths for another run
            if not os.path.exists(self.tmp_dir):
                os.makedirs(self.tmp_dir)
            with open(widths_pickle, 'wb') as f:
                pickle.dump(widths, f, protocol=pickle.HIGHEST_PROTOCOL)

        print("Got widths in %.2f seconds" % (end_time - start_time,))
        return widths

    def read_widths(self):
        # Use multiple processes to read the file
        processes = 9
        manager = mp.Manager()
        queue = manager.Queue()
        pool = mp.Pool(processes=processes)

        # Start a listener to consume output from the workers
        listener = pool.apply_async(FixedTable.widths_consumer, (queue,))

        # Start workers to read chunks of the input_csv
        jobs = []
        rows = self.input_size / ((processes - 1) * 2)
        print("Splitting %d bytes between 8 processes in chunks of %d" % (self.input_size, rows))
        for i in range(8):
            start_pos = i * rows
            end_pos = start_pos + rows
            if end_pos > self.input_size:
                end_pos = self.input_size
            args = (queue, self.input_csv, start_pos, end_pos)
            job = pool.apply_async(FixedTable.widths_producer, args)
            jobs.append(job)

        # Wait for jobs to finish
        for job in jobs:
            job.get()

        # Notify listener to stop listening and receive aggegrate from listener
        queue.put(-1)
        widths = listener.get()

        # Clean up
        pool.close()
        return widths

    @staticmethod
    def widths_producer(queue, csv, start_pos, end_pos):
        print("Reading widths from %d to %d" % (start_pos, end_pos), flush=True)
        with open(csv, 'r') as f:
            position = f.seek(start_pos)
            widths = defaultdict(int)
            while position < end_pos:
                # Read a line from the csv without \n
                line = f.readline()[:-1]

                # Record the maximum length by column index
                for i, val in enumerate(line.split(',')):
                    width = len(val)
                    if width > widths[i]:
                        widths[i] = width

                # Update the position in the file
                position = f.tell()
        queue.put(widths)
        return widths

    @staticmethod
    def widths_consumer(queue):
        widths = defaultdict(int)
        stop = False
        while not queue.empty() or  not stop:
            chunk_widths = queue.get()
            if chunk_widths == -1:
                stop = True
            else:
                for k in chunk_widths:
                    if chunk_widths[k] > widths[k]:
                        widths[k] = chunk_widths[k]
        return widths


    def write(self, output_csv):
        # Record running time
        start_time = time.time()

        # Read from the input csv and write to new fixed width csv
        in_file = open(self.input_csv, 'rb')
        out_file = open(output_csv, 'wb')

        # Write the column headers
        out_file.write(in_file.readline())

        # Write each line with justified values
        position = in_file.tell()
        while position < self.input_size:
            line_bytes = in_file.readline()[:-1]
            line = line_bytes.decode().split(',')
            fixed = [v.rjust(self.widths[i]) for i, v in enumerate(line)]
            fixed_line = ','.join(fixed)+'\n'
            position += out_file.write(fixed_line.encode())

        # Close the files
        in_file.close()
        out_file.close()

        # Print run time
        end_time = time.time()
        print("Wrote fixed width csv in %.2f seconds" % (end_time - start_time,))

    def column_operation(self, csv, first_column, second_column, operation):
        """
        Performs a WHERE comparison over the columns of the csv

        "FROM out.csv WHERE asdf < fdsa"
        column_operation("out.csv", "asdf", "fdsa", Operators.LessThan)

        param: csv: csv file to query
        param: first_column: column name as string
        param: second_column: column name as string
        param: operation: comparison to make over values
        """
        # Record the running time
        start_time = time.time()

        # Use multiple processes to read the file
        processes = 9
        manager = mp.Manager()
        queue = manager.Queue()
        pool = mp.Pool(processes=processes)

        # Look up the starting position and width of the columns in a row
        col1 = 0
        col1_width = 0
        col2 = 0
        col2_width = 0
        line_length = 0
        f = open(csv, 'r')
        column_line = f.readline()
        columns = column_line[:-1].split(',')
        f.close()
        for i, k in enumerate(columns):
            inc = self.widths[i]
            if k == first_column:
                col1 = line_length
                col1_width = inc
            elif k == second_column:
                col2 = line_length
                col2_width = inc
            line_length += inc + 1

        # Start a listener aggregate results
        listener = pool.apply_async(FixedTable.operation_aggregator, (queue, csv))

        # Start jobs to compare column values
        start = len(column_line)
        fixed_size = os.path.getsize(csv) - start
        chunks = 8
        chunksize = fixed_size // chunks
        chunksize = chunksize // line_length * line_length
        jobs = []
        for i in range(chunks):
            start_pos = chunksize * i + start
            end_pos = start_pos + chunksize
            if i == chunks - 1:
                end_pos = fixed_size
            args = (queue, csv, start_pos, end_pos, col1, col1_width, col2, col2_width, line_length)
            j = pool.apply_async(FixedTable.operation_comparator, args)
            jobs.append(j)

        # Wait for jobs to finish
        for job in jobs:
            job.get()

        # Notify listener to stop listening and receive aggegrate from listener
        queue.put(-1)
        rows = listener.get()

        # Clean up
        pool.close()

        # Print the running time
        end_time = time.time()
        print('Performed operation in %.3f seconds' % (end_time - start_time,))
        return rows

    @staticmethod
    def operation_aggregator(queue, csv):
        lines = []
        stop = False
        f = open(csv, 'r')
        while not queue.empty() or not stop:
            row_byte_index = queue.get()
            if row_byte_index == -1:
                stop = True
            else:
                for index in row_byte_index:
                    f.seek(index)
                    lines.append(f.readline()[:10])
        f.close()
        return lines

    @staticmethod
    def operation_comparator(queue, csv, start_pos, end_pos, col1, col1_width, col2, col2_width, line_length):
        print("Comparing from %d to %d" % (start_pos, end_pos), flush=True)

        # Make sure that the first column is actually first
        if col2 < col1:
            col1, col2 = col2, col1
            col1_width, col2_width = col2_width, col1_width

        # Seek through the columns
        results = []
        with open(csv, 'r') as f:
            position = f.seek(start_pos)
            while position < end_pos:
                f.seek(position + col1)
                v1 = int(f.read(col1_width))
                f.seek(position + col2)
                v2 = int(f.read(col2_width))
                if v1 == v2:
                    results.append(position)
                position = f.seek(position + line_length)
            f.close()

        queue.put(results)
        return results
