from process_framework import Reference, Step
from abc import abstractmethod
# from collections import deque
from typing import Iterable
from dataclasses import dataclass, KW_ONLY, field
import logging
from .composition.core import HasInput
from ..composition.retries import RetryArgs
from itertools import batched
# @dataclass(slots=True)
# class _Retry[TBatch]:
#     i:int
#     batch:TBatch
#     try_:int = 0


@dataclass
class BatchProcessor[TIn, TBatch](Step, HasInput[TIn]):
    """ apply a list of steps to a batch """
    batch:Reference[TBatch] # the batch
    steps:list[Step]        # the steps applied to the batch
    batch_size:int          # the size of the batch
    _ : KW_ONLY
    retry:RetryArgs|None
    


    def gen_batches(self, input_:TIn) -> Iterable[TBatch]:
        if not isinstance(input_, Iterable):
            raise ValueError()

        return batched(input_, self.batch_size) 
    

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

        for i, batch in batches:
            call = lambda batch=batch: self.handle_batch(batch)
            if self.retry:
                call = self.retry.wrap(call)
            # call.__setattr__('batch_index', i)
            call.__name__ = f'batch {i}'
            call()


        logging.info('done!')
    

    def preflight(self):
        """ perform preflight for nested steps """
        for step in self.steps:
            step.preflight()





        # # prepare a collection of retries
        # to_retry:deque[_Retry[TBatch]] = deque()


        #     try:
        #         self.handle_batch(batch)
        #     except Exception as e:
        #         retry = _Retry(i, batch)
        #         self.on_batch_error(retry, e)
        #         to_retry.append(retry)

        # # while there are _Retries handle them until their `try_` exceeds `self.max_retries`
        # while to_retry:
        #     retry = to_retry.popleft()
        #     try:
        #         logging.info(f"retrying, {retry.i}, try={retry.try_ + 1}/{self.max_retries}")
        #         self.handle_batch(retry.batch)
            
        #     except Exception as e:
        #         if retry.try_ >= self.max_retries:
        #             logging.info(f'retries exhausted for batch {retry.i}')
        #             raise
                
        #         logging.info(f'{retry.i}, {e}')
        #         retry.try_ += 1
        #         to_retry.append(retry)