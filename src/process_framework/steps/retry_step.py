from .step import Step
from itertools import count
import logging
from time import sleep

class Retry(Step):
    """ retry a `step` up to `max_retries` times, waiting `retry_backoff` seconds between tries """
    step:Step
    max_retries:int
    retry_backoff:int

    def do(self):
        for i in range(self.max_retries):
            try:
                # try to do the wrapped step, if it succeeds, break
                self.step.do()
                break
            except Exception as e:
                logging.warning(f'retry: {i}: {e} (sleeping for {self.retry_backoff}s)')

                # if we're on the last retry, raise the exception
                if i == (self.max_retries - 1):
                    raise e
                
                sleep(self.retry_backoff)
