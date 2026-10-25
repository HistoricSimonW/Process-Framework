from __future__ import annotations
from ..reference import IGettable, ISettable
from ...import resolve, ValueOrReference
from pandas import Series, DataFrame, Index
from typing import cast
from dataclasses import dataclass


@dataclass
class ColumnReference[T:Series](IGettable[T], ISettable[T]):
    """ a reference to a column in the value of a Reference[DataFrame] """
    df:ValueOrReference[DataFrame]
    column:str
    column_as_index:str|ColumnReference|None=None
    type: type[T] = cast(type[T], Series)
    
    
    def has_value(self) -> bool:
        df = resolve(self.df)
        return df is not None and (self.column in df.columns)
    

    def get_value(self) -> T:
        value = resolve(self.df)[self.column]
        return cast(T, value)
    
    
    def get_type(self) -> type[T]:
        return self.type
    
    
    def _get_setter_mapping_keys(self) -> Index|Series:
        df = resolve(self.df)
        
        if self.column_as_index is None:
            return df.index
        
        if isinstance(self.column_as_index, str):
            return df[self.column_as_index]

        if isinstance(self.column_as_index, ColumnReference):
            return df[self.column_as_index.column]
        
        raise Exception()


    def _on_set(self, value: T | None) -> None:
        try:
            df = resolve(self.df)
        except Exception as e:
            raise ValueError("referenced dataframe has not been assigned; we can't assign a value to a column in it") from e
        
        if value is None:
            df[self.column] = None
            return

        mappable = self._get_setter_mapping_keys()
        
        df[self.column] = mappable.map(value) # type:ignore