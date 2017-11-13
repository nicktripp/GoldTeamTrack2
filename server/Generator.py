import random
import multiprocessing as mp
import os
import sys

class Generator:
    @staticmethod
    def worker(queue, generators):
        for i in range(1000):
            data = [','.join(str(generators[column]()) for column in generators)]
            lines = '\n'.join(data) + '\n'
            queue.put(lines)
        return lines

    @staticmethod
    def listener(queue):
        if not os.path.exists('./data/'):
            os.makedirs('./data/')
        with open('./data/out.csv', 'w', newline='') as f:
            while 1:
                m = queue.get()
                if m == -1:
                    break
                f.write(m)
            f.flush()
            os.fsync(f.fileno())

    @staticmethod
    def generate(rows, generators):
        # Make a manager that uses a pool of processes
        manager = mp.Manager()
        queue = manager.Queue()
        pool = mp.Pool(processes=8)

        # put listener to work first
        watcher = pool.apply_async(Generator.listener, (queue,))

        # write the column headers
        queue.put(','.join(sorted(generators.keys()))+'\n')

        # start workers
        jobs = []
        for i in range(rows):
            job = pool.apply_async(Generator.worker, (queue, generators))
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
                generators[alphabet[i] + alphabet[j]] = Generator.get_random_int
        return generators

    @staticmethod
    def get_random_int():
        return random.randint(1, 999)
