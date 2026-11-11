from dataclasses import dataclass
from process_framework import TransformingStep
from pandas import DataFrame
from typing import Iterable

@dataclass
class BatchProcessDataFrame(TransformingStep[DataFrame, Iterable[DataFrame]]):
    batch_size:int
    
    def gen_batches(self, subject: DataFrame) -> Iterable[DataFrame]:
            n = len(subject.index)
            for start in range(0, n, self.batch_size):
                yield subject.iloc[start : start + self.batch_size, :].copy()
                
                
    def transform_value(self, input_: DataFrame) -> Iterable[DataFrame]:
         return self.gen_batches(input_)