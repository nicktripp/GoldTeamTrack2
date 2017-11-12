import random
import multiprocessing as mp
import os


class CsvGenerator:
    @staticmethod
    def worker(q, generators):
        for i in range(1000):
            data = [','.join(str(generators[column]()) for column in generators)]
            lines = '\n'.join(data) + '\n'
            q.put(lines)
        return lines

    @staticmethod
    def listener(q):
        if not os.path.exists('../../data/'):
            os.makedirs('../../data/')
        with open('../../data/out.csv', 'w+') as f:
            while 1:
                m = q.get()
                if m == -1:
                    break
                f.write(m)
                f.flush()
            f.close()

    @staticmethod
    def generate(rows, generators):
        # Make a manager that uses a pool of processes
        manager = mp.Manager()
        queue = manager.Queue()
        pool = mp.Pool(8)

        # put listener to work first
        watcher = pool.apply_async(CsvGenerator.listener, (queue,))

        # start workers
        jobs = []
        for i in range(rows):
            job = pool.apply_async(CsvGenerator.worker, (queue, generators))
            jobs.append(job)


        # collect results from the workers through the pool result queue
        for job in jobs:
            job.get()
        queue.put(-1)
        pool.close()

    @staticmethod
    def generate_random_int_cols():
        alphabet = 'abcedfjhijklmnopqrstuvwxyz'
        generators = {}
        for i in range(18):
            for j in range(18):
                generators[alphabet[i] + alphabet[j]] = CsvGenerator.get_random_int
        return generators

    @staticmethod
    def get_random_int():
        return random.randint(1, 1001)

if __name__ == "__main__":
    # Generate 289 random attributes
    generators = CsvGenerator.generate_random_int_cols()
    CsvGenerator.generate(1000, generators)
