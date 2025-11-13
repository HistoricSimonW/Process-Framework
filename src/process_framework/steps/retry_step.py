from .step import Step
from itertools import count
import logging

class Retry(Step):
    # TODO: catch certain Exceptions, re-raise the rest
    """ retry a `step` up to `max_retries` times, waiting `retry_backoff` seconds between tries """
    def __init__(self, step:Step, max_retries:int=10, retry_backoff:int=15) -> None:
        super().__init__()
        self.step = step
        self.max_retries = max_retries
        self.retry_backoff=retry_backoff


    def do(self):
        for i in range(self.max_retries):
            try:
                # try to do the wrapped step, if it succeeds, break
                self.step.do()
                break
            except Exception as e:
                logging.warning(f'retry: {i}: {e}')

                # if we're on the last retry, raise the exception
                if i == (self.max_retries - 1):
                    raise e
