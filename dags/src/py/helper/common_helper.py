import time
from functools import wraps

def isnan(item):
    return item != item

def check_duration(func):
    @wraps(func)
    def check_duration_warpper(*args, **kwargs):
        print("\nStart Work %s " % (func.__name__))
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print("Finish Work %s " % (func.__name__))
        print(f'Duration : {total_time} seconds')
        return result
    return check_duration_warpper