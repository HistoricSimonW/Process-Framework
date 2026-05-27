from pydantic import BaseModel
from dataclasses import dataclass
from typing import Iterable, Callable, Any
from pandas import DataFrame, Series
from .pydantic import GenerateModelsFromDataFrame, GenerateModelsFromIterable
from .elasticsearch.document import DocumentBase


def _index_as_id(index:Any, row:Series) -> Any:
    return index

class GenerateDocumentsFromIterable[TIn, TOut:DocumentBase](GenerateModelsFromIterable[TIn, TOut]):
    id_func:Callable[[TIn], Any]
    kwargs_func:Callable[[TIn], dict]

    def get_kwargs_for_item(self, item: TIn) -> dict:
        return self.kwargs_func(item)

    def get_model_for_item(self, item: TIn) -> TOut:
        _id = self.id_func(item)
        args = self.kwargs_func(item)
        args['_id'] = _id
        return self.model_type.model_validate(args)
        
    

class GenerateDocumentsFromDataFrame[T:DocumentBase](GenerateModelsFromDataFrame[T]):
    id_func:Callable[[Any, Series], Any] = _index_as_id
    
    def row_to_model(self, index: Any, row: Series[Any]) -> T:
        _id = self.id_func(index, row)
        args = row.to_dict()
        args['_id'] = _id
        return self.model_type.model_validate(args)
            


