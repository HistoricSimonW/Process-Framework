from collections.abc import Callable
from functools import partial
from typing import Any

from pandas import DataFrame, Series
from pydantic import BaseModel


type DataFrameTransformer[T] = Callable[[DataFrame], T]


type RowFunc[T] = (
    Callable[[Series], T]
    | Callable[[Any, Series], T]
)


def _normalize_row_func(row_func:Callable, with_id:bool) -> Callable[[Any, Series], Any]:
    """Normalize a row function to accept an index and row."""
    if with_id:
        # if row_func has one required argument, return a lambda that discards id
        return row_func
    
    return lambda _, row: row_func(row)

    
def transform_rows[TValue, TOut](df:DataFrame, row_func:RowFunc[TValue], result_func:Callable[[dict[Any, TValue]], TOut], with_id:bool = False) -> TOut:
    """Transform DataFrame rows and construct the result."""
    func = _normalize_row_func(row_func, with_id)
     
    rows = {
        i: func(i, row)
        for i, row in df.iterrows()
    }
        
    return result_func(rows)
        

def as_rows[TValue, TOut](row_func: RowFunc[TValue], result_func: Callable[[dict[Any, TValue]], TOut], with_id: bool = False, ) -> DataFrameTransformer[TOut]:
    """Return a DataFrame transformer that applies a function to each row."""
    return partial(
        transform_rows,
        row_func=row_func,
        result_func=result_func,
        with_id=with_id,
    )
    
    
def column_as_series(df:DataFrame, column:str) -> Series:
    """Extract a DataFrame column as a Series, raising if it does not exist."""
    if column not in df.columns:
        raise KeyError(
            f"Column {column!r} not found; "
            f"available columns: {df.columns.tolist()}"
        )
    
    return df[column]

    
def column_as_set(df:DataFrame, column:str) -> set:
    """Extract the unique values of a DataFrame column as a set."""
    s = column_as_series(df, column)
    return set(s)


def as_series(column:str) -> DataFrameTransformer[Series]:
    """Return a transformer that extracts a DataFrame column as a Series."""
    return partial(
        column_as_series,
        column=column
    )


def as_transformed_series[T](value_func:Callable[[Series], T]) -> DataFrameTransformer[Series]:
    """Return a transformer that applies a function to each row."""
    return as_rows(
        row_func=value_func,
        result_func=Series
    )
    

def as_model_series[T:BaseModel](model_type:type[T]) -> DataFrameTransformer[Series]:
    """Return a transformer that validates each row as a Pydantic model."""
    return as_transformed_series(
        value_func=lambda row: model_type.model_validate(row.to_dict())
    )
    
    
def as_set(column:str) -> DataFrameTransformer[set]:
    """Return a transformer that extracts unique column values as a set."""
    return partial(
        column_as_set,
        column=column
    )
    

