from ...references import Reference
from ..step import TransformingStep
from pydantic import BaseModel
from pandas import DataFrame, Series
from typing import Any, Type
from dataclasses import dataclass

@dataclass
class DataFrameToModels[T:BaseModel](TransformingStep[DataFrame, Series]):
    document_type:Type[T]

    def transform_value(self, input_: DataFrame) -> Series[Any]:
        df = input_.copy()
        
        # if the df has a named index, get its levels as columns
        if all(df.index.names):
            for i, n in enumerate(df.index.names):
                df[n] = df.index.get_level_values(i)

        # construct documents by passing each row (axis=1) of the dataframe to the `document_type` constructor
        return df.apply(lambda row: row.dropna().to_dict(), axis=1).map(self.document_type.model_validate)