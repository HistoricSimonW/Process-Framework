from typing import Iterable, Callable, Any
from process_framework.steps.composition.elasticsearch.core import Hit
from .core import AssignScanResult
from pandas import DataFrame
from pydantic import BaseModel
from dataclasses import dataclass

def _get_id(hit:Hit) -> str:
    return hit.id

@dataclass
class AssignScanResultDataFrame[T:BaseModel](AssignScanResult[T, DataFrame]):
    """assign scanned hits as a pandas DataFrame."""
    set_index:str|None=None
    id_func:Callable[[Hit[T]], Any] = _get_id
    id_name:str='_id'

    def transform_value(self, input_: Iterable[Hit[T]]) -> DataFrame:
        """materialize scanned hits as a DataFrame."""
        rows = []
        index = []

        for hit in input_:
            rows.append(hit.source.model_dump())
            index.append(self.id_func(hit))

        df = DataFrame(rows, index=index)
        df.index.name = self.id_name
        
        if self.set_index is not None:
            df = df.reset_index().set_index(self.set_index)
        
        return df
    

