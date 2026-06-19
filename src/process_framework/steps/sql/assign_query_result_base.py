from process_framework import Reference, AssigningStep
from abc import ABC
from pandas import DataFrame, Series, Index
from typing import Mapping, Callable, cast, Any
from ..composition.sql import BuildsQueryBase, ProvidesQueryResults
from dataclasses import dataclass


@dataclass(kw_only=True)
class GetSqlQueryResultBase[T:(DataFrame, Series, Index)](ProvidesQueryResults, BuildsQueryBase, AssigningStep[T], ABC):
    """ base class for Steps that assign the result of Sql queries to `assign_to`"""
    column_as_index:str|list[str]|None=None
    column_mapper:Callable[[str], str]|Mapping[str,str]|None=None
    column_as_series:str|None=None
    drop_index_column:bool=True
    dtypes:dict[str, Any]|type|str|None=None


    def _cast_result_to_type(self, result:DataFrame) -> T:
        """ handle the result DataFrame into the required output type """
        output_type = self.output_.get_type()

        if output_type == DataFrame:
            return cast(T, result)
        
        if output_type == Index and (
            (isinstance(self.column_as_index, str) and self.column_as_index in result.index.names) or
            (isinstance(self.column_as_index, list) and all(t in result.index.names for t in self.column_as_index))
        ):
            return cast(T, result.index)
        
        if output_type == Series and self.column_as_series is not None:
            if len(result.columns) == 0:
                self._warn("got result with zero columns, returning an empty Series")
                return cast(T, Series([], name=self.column_as_series))
            
            if self.column_as_series in result.columns:
                return cast(T, result[self.column_as_series])
            else:
                self._warn(f"column `{self.column_as_series}` was not a name in `result`'s columns; has it been changed by `column_mapper` {self.column_mapper}?")

        raise Exception(f"`assign_to` expects a `{output_type}`, but `result` is {type(result)}")


    def on_transform_result(self, result:DataFrame) -> DataFrame:
        """ overwrite this to modify the query result DataFrame before it is cast to the result type
            this is the place to do any bespoke text transformations or other conditional logic """
        return result
    

    def transform_result(self, result:DataFrame) -> T:
        """ apply renames and indexing, 
            pass the result through `on_transform_result` 
            then return the typed result via `_cast_result_to_type` """
        if self.column_mapper is not None:
            result = result.rename(self.column_mapper, axis=1)
        
        if self.dtypes is not None:
            result = result.astype(self.dtypes) # type:ignore

        if self.column_as_index is not None:
            result = result.set_index(self.column_as_index, drop=self.drop_index_column)

        result = self.on_transform_result(result)

        return self._cast_result_to_type(result)

    
    def generate_value(self) -> T | None:
        query = self.get_modified_query(self)
        result = self.get_query_result(query)
        self._info(f'got {len(result)} records as a {type(result).__name__}')
        return self.transform_result(result)
