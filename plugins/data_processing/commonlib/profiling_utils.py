import functools
import time as _time

def _printer(s):
    print(s)


def timeit(log_helper=_printer):
    def timing_decorator(func):
        @functools.wraps(func)
        def time_execution(*args, **kwargs):
            start_time = _time.time()
            value = func(*args, **kwargs)
            end_time = _time.time()
            run_time = end_time - start_time
            log_helper("%r finished in %.3f seconds" % (func.__name__, run_time))
            return value
        return time_execution
    return timing_decorator


@timeit()
def _waste_some_time(num_times):
    for _ in range(num_times):
        sum([i**2 for i in range(10000)])


if __name__ == '__main__':
    _waste_some_time(1000)
