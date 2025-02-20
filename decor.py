import time

def time_pars(func):
    def wrapper(*args, **kwargs):
        start = time.time()

        func(*args, **kwargs)

        print(time.time() - start)

    return wrapper