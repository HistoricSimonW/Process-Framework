from contextlib import contextmanager
from time import sleep

@contextmanager
def retries(*exceptions:type[Exception], retries:int, backoff:float, raise_on_exhausted:Exception|None):
    try:
        yield
    finally:
        ...
