from ...references.reference import Reference
from ..transforming_step import TransformingStep
from .document import Document
from pandas import DataFrame, Series
from typing import Any, Type


class DataFrameToDocuments[T:Document](TransformingStep[DataFrame, Series]):
    def __init__(self, subject: Reference[DataFrame], assign_to: Reference[Series], document_type:type):
        super().__init__(subject, assign_to)
        self.document_type:Type[T] = document_type


    def transform(self, subject: DataFrame) -> Series | None:
        df = subject.copy()
        
        # if the df has a named index, get its levels as columns
        if all(df.index.names):
            for i, n in enumerate(df.index.names):
                df[n] = df.index.get_level_values(i)

        # construct documents by passing each row (axis=1) of the dataframe to the `document_type` constructor
        return df.apply(lambda row: row.dropna().to_dict(), axis=1).map(self.document_type.model_validate)