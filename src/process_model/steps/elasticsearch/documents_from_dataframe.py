from ...references.reference import Reference
from ..transforming_step import TransformingStep
from .document import Document
from pandas import DataFrame, Series
from typing import Any, Type


class DocumentsFromDataframe[T:Document](TransformingStep[DataFrame, Series]):
    def __init__(self, subject: Reference[DataFrame], assign_to: Reference[Series], _type:type):
        super().__init__(subject, assign_to)
        self.cls:Type[T] = _type


    def transform(self, subject: DataFrame) -> Series | None:
        df = subject.copy()
        
        if all(df.index.names):
            for i, n in enumerate(df.index.names):
                df[n] = df.index.get_level_values(i)

        return df.apply(lambda row: self.cls(**row.to_dict()), axis=1)

