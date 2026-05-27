from pydantic import BaseModel
from dataclasses import dataclass
from ..step import TransformingStep
from typing import Iterable, Any
from pandas import DataFrame, Series

@dataclass
class GenerateModels[TIn, TOut:BaseModel](TransformingStep[TIn, Iterable[TOut]]):
    # e.g., generate docs from a DataFrame
    model_type:type[TOut]
    
    def model_validate(self, args:dict) -> TOut:
        return self.model_type.model_validate(args)


@dataclass
class GenerateModelsFromIterable[TIn, TOut:BaseModel](GenerateModels[Iterable[TIn], TOut]):
    def get_model_for_item(self, item:TIn) -> TOut:
        return self.model_type.model_validate(item)
    
    def transform_value(self, input_: Iterable[TIn]) -> Iterable[TOut]:
        for item in input_:
            yield self.get_model_for_item(item)


@dataclass
class GenerateModelsFromDataFrame[T:BaseModel](GenerateModels[DataFrame, T]):
    
    def row_to_model(self, index:Any, row:Series) -> T:
        args = row.to_dict()
        args['index'] = index
        return self.model_type.model_validate(args)
    

    def transform_value(self, input_: DataFrame) -> Iterable[T]:
        for i, row in input_.iterrows():
            yield self.row_to_model(i, row)