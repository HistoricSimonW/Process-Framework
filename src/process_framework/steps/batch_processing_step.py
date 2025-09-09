from process_framework import Reference, Step
from process_framework.steps import TransformingStep
from abc import ABC, abstractmethod
from pandas import DataFrame, Series
from itertools import batched
from typing import Any, TypeVar, Type
from types import SimpleNamespace
from collections.abc import Iterable
from argparse import Namespace


TIn = TypeVar("TIn")
TBatch = TypeVar("TBatch")
TOut = TypeVar("TOut")


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
                yield subject.iloc[start : start + self.batch_size]  # type: ignore
            return

        if isinstance(subject, DataFrame) and self._batch_type is DataFrame:
            n = len(subject.index)
            for start in range(0, n, self.batch_size):
                yield subject.iloc[start : start + self.batch_size, :]  # type: ignore
            return

        if isinstance(subject, Iterable) and self._batch_type is list:
            for batch in batched(subject, self.batch_size):
                yield list(batch)  # type: ignore
            return
        
        raise ValueError("Unhandled TSubject -> TBatch generaiton")

    
    def process_batch(self) -> Any|None:
        """ apply `self.steps` to the current `batch` """
        for step in self.steps:
            step.do()

    
    def get_output(self) -> TOut | None:
        """ get a `TOut` output or `None`; usually the Subprocess should include an `Append` step, and the output should be computed from the related list """ 
        return None
    

    def transform(self, subject: TIn) -> TOut | None:
        """ transform the subject by generating batches from `subject`, assigning each batch to `self.batch`, then applying `process_batch` """
        for i, batch in enumerate(self.gen_batches(subject)):
            self.batch.set(batch)
            result = self.process_batch()
            print(i, result)

        return self.get_output()