from pydantic import BaseModel
from pandas import DataFrame, Series
from typing import Any, Type, Iterable
from dataclasses import dataclass
from process_framework.helpers.pydantic import gen_models_from_records
from process_framework import Reference, TransformingStep

@dataclass
class GetModelsForDataFrame[T:BaseModel](TransformingStep[DataFrame, Series]):
    model_type:Type[T]

    def transform_value(self, input_: DataFrame) -> Series:
        df = input_.copy()
        
        # if the df has a named index, get its levels as columns
        if all(df.index.names):
            for i, n in enumerate(df.index.names):
                df[n] = df.index.get_level_values(i)

        # construct documents by passing each row (axis=1) of the dataframe to the `document_type` constructor
        return df.apply(lambda row: row.dropna().to_dict(), axis=1).map(self.model_type.model_validate)
    

@dataclass
class GenModelsForDataFrame[T:BaseModel](TransformingStep[DataFrame, Iterable[T]]):
    model_type:Type[T]

    def transform_value(self, input_: DataFrame) -> Iterable[T]:
        df = input_.copy()

        # if the df has a named index, get its levels as columns
        if all(df.index.names):
            for i, n in enumerate(df.index.names):
                df[n] = df.index.get_level_values(i)

        
        def gen_records() -> Iterable[dict]:
            for _, row in df.iterrows():
                yield row.dropna().to_dict()

        return gen_models_from_records(
            type=self.model_type,
            records=gen_records()
        )