import time
from functools import wraps


def timeit(timer_tag):
    def func_wrapper(func):
        @wraps(func)
        def returned_wrapper(*args, **kwargs):
            print(timer_tag + ": Starting Timer...")
            t0 = time.time()
            result = func(*args, **kwargs)
            t1 = time.time()
            print(timer_tag+": Complete\n\t|- Time Elapsed :: %f s" % (t1 - t0))
            return result

        return returned_wrapper

    return func_wrapper