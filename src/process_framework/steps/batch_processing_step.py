from process_framework import Reference, Step
from process_framework.steps import TransformingStep
from abc import ABC, abstractmethod
from pandas import DataFrame, Series
from itertools import batched
from typing import Any, TypeVar, Type
from collections import deque
from collections.abc import Iterable
from types import SimpleNamespace
from argparse import Namespace
from dataclasses import dataclass


TIn = TypeVar("TIn")
TBatch = TypeVar("TBatch")
TOut = TypeVar("TOut")

    
@dataclass
class _Retry[TBatch]:
    i:int
    batch:TBatch
    try_:int



class BatchProcess[TIn, TBatch, TOut](TransformingStep[TIn, TOut], ABC):
    """ Apply a sequence of `Steps` to `batches` of `TBatch` """
    
    def __init__(self, subject: Reference[TIn], assign_to: Reference[TOut], args:Namespace, refs:SimpleNamespace, clients:SimpleNamespace, *, 
                 batch_type:Type[TBatch], 
                 batch_size:int|None=None):
        super().__init__(subject, assign_to)
        self.batch = Reference(batch_type, None)
        self.args = args
        self.refs = self.initialize_references(args=args, outer_refs=refs)
        self.steps = self.initialize_steps(args=args, refs=self.refs, outer_refs=refs, clients=clients)
        self._batch_type:Type[TBatch] = batch_type
        
        self.max_retries = 25
        
        if isinstance(batch_size, int) or (batch_size := args.batch_size):
            self.batch_size = batch_size
        else:
            raise Exception()

    @abstractmethod
    def initialize_steps(self, args:Namespace, refs:SimpleNamespace, outer_refs:SimpleNamespace, clients:SimpleNamespace) -> list[Step]:
        pass

    
    @abstractmethod
    def initialize_references(self, args:Namespace, outer_refs:SimpleNamespace) -> SimpleNamespace:
        pass


    # @abstractmethod
    def gen_batches(self, subject:TIn) -> Iterable[TBatch]:
        # some default behaviours
        if isinstance(subject, Series) and self._batch_type is Series:
            n = len(subject)
            for start in range(0, n, self.batch_size):
                yield subject.iloc[start : start + self.batch_size].copy()  # type: ignore
            return

        if isinstance(subject, DataFrame) and self._batch_type is DataFrame:
            n = len(subject.index)
            for start in range(0, n, self.batch_size):
                yield subject.iloc[start : start + self.batch_size, :].copy()  # type: ignore
            return

        if isinstance(subject, Iterable) and self._batch_type is list:
            for batch in batched(subject, self.batch_size):
                yield list(batch)  # type: ignore
            return
        
        raise ValueError("Unhandled TSubject -> TBatch generaiton")

    
    def apply_steps(self) -> Any|None:
        """ apply `self.steps` to the current `batch` """
        for step in self.steps:
            step.do()


    def handle_batch(self, batch:TBatch):
        self.batch.set(batch)
        self.apply_steps()

    
    def get_output(self) -> TOut | None:
        """ get a `TOut` output or `None`; usually the Subprocess should include an `Append` step, and the output should be computed from the related list """ 
        return None
    


    def transform(self, subject: TIn) -> TOut | None:
        """ transform the subject by generating batches from `subject`, assigning each batch to `self.batch`, then applying `process_batch` """
        to_retry:deque[_Retry] = deque()

        # try all the batches
        for i, batch in enumerate(self.gen_batches(subject)):
            try:
                self.handle_batch(batch)
            except Exception as e:
                print(i, e)
                to_retry.append(_Retry(i, batch, 0))

        # while there are _Retries handle them until their `try_` exceeds `self.max_retries`
        while to_retry:
            retry = to_retry.popleft()
            try:
                print("retrying", retry.i, f"(try={retry.try_ + 1}/{self.max_retries})")
                self.handle_batch(retry.batch)
            
            except Exception as e:
                if retry.try_ >= self.max_retries:
                    print(f'retries exhausted for batch {retry.i}')
                    raise
                
                print(retry.i, e)
                retry.try_ += 1
                to_retry.append(retry)

        if to_retry:
            raise Exception(f"Retries were exhausted; {len(to_retry)} batches unhandled")
        
        return self.get_output()
    
