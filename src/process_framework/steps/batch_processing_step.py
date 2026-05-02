from process_framework import Reference, Step
from abc import abstractmethod
from pandas import DataFrame
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, KW_ONLY
import logging
from .composition.core import HasInput

@dataclass(slots=True)
class _Retry[TBatch]:
    i:int
    batch:TBatch
    try_:int = 0


@dataclass
class BatchProcessor[TIn, TBatch](Step, HasInput[TIn]):
    """ apply a list of steps to a batch """
    batch:Reference[TBatch]
    steps:list[Step]
    batch_size:int
    _ : KW_ONLY
    max_retries:int = 10


    @abstractmethod
    def gen_batches(self, subject:TIn) -> Iterable[TBatch]:
        ...
    

    def on_batch_error(self, retry: _Retry[TBatch], exc: Exception) -> None:
        # default: just log; override in subclass for backoff, metrics, etc.
        self._warn(f"batch {retry.i} failed (try={retry.try_}): {exc}")


    def handle_batch(self, batch:TBatch) -> None:
        self.batch.set_value(batch)
        try:
            for step in self.steps:
                step.do()
        finally:
            # clear the batch on successful completion or on error
            self.batch.set_value(None)


    def do(self) -> None:
        # get the subject; throw an error if it's not available
        subject = self.input_.get_value()

        # generated an enumerated iterable of batches
        batches = enumerate(self.gen_batches(subject))

        # prepare a collection of retries
        to_retry:deque[_Retry[TBatch]] = deque()

        for i, batch in batches:
            try:
                self.handle_batch(batch)
            except Exception as e:
                retry = _Retry(i, batch)
                self.on_batch_error(retry, e)
                to_retry.append(retry)

        # while there are _Retries handle them until their `try_` exceeds `self.max_retries`
        while to_retry:
            retry = to_retry.popleft()
            try:
                logging.info(f"retrying, {retry.i}, try={retry.try_ + 1}/{self.max_retries}")
                self.handle_batch(retry.batch)
            
            except Exception as e:
                if retry.try_ >= self.max_retries:
                    logging.info(f'retries exhausted for batch {retry.i}')
                    raise
                
                logging.info(f'{retry.i}, {e}')
                retry.try_ += 1
                to_retry.append(retry)

        logging.info('done!')
    

    def preflight(self):
        """ perform preflight for nested steps """
        for step in self.steps:
            step.preflight()


@dataclass
class BatchProcessDataFrame(BatchProcessor[DataFrame, DataFrame]):

    def gen_batches(self, subject: DataFrame) -> Iterable[DataFrame]:
            n = len(subject.index)
            for start in range(0, n, self.batch_size):
                yield subject.iloc[start : start + self.batch_size, :].copy()