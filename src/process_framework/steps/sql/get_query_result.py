from typing import Callable, Mapping, Any, cast
from functools import partial
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass

from pandas import DataFrame, read_sql
from sqlalchemy import Connection, Select, Table, select, ColumnElement, Engine
from inflection import underscore

from process_framework import AssigningStep
from .transformation.dataframe import DataFrameTransformer


type ColumnMapperType = Mapping | Callable[[Any], Any]

type QueryContextManager = Callable[
        [Connection],
        AbstractContextManager[ColumnElement[bool]]
    ]


def _map_name(name: Any, mapper: ColumnMapperType) -> Any:
    """Map a name using a callable or mapping, preserving None and unmapped names."""
    if name is None:
        return None

    if callable(mapper):
        return mapper(name)

    return mapper.get(name, name)


def _map_columns(df: DataFrame, column_mapper: ColumnMapperType) -> None:
    """Rename DataFrame columns in place using the supplied mapper."""
    df.columns = df.columns.map(
        partial(_map_name, mapper=column_mapper)    
    )


@dataclass(kw_only=True)
class GetQueryResult[T](AssigningStep[T]):
    """Execute a SQL query and assign its result as a DataFrame or transformed value."""
    query:Select|Table
    connection:Connection|Engine
    column_mapper:ColumnMapperType|None=underscore
    """ a dataframe column mapper, passed to applied to the result before .astype and .set_index are called """
    index_col:str|list[str]|None=None
    """ a single or sequence of mapped column names to use as the result dataframe's index """
    index_drop:bool=False
    dtype:str|Mapping|None=None
    """ an argument passed to df.astype on the result dataframe, using mapped column names """
    limit:int|None=None
    where:ColumnElement[bool]|None=None
    query_context: QueryContextManager|None=None
    df_func:DataFrameTransformer[T]|None = None
    
    def get_query(self, *where:ColumnElement[bool]|None) -> Select:
        """Build the SQL SELECT statement with optional filtering and row limits."""
        query = self.query
                
        if not isinstance(query, Select):
            query = select(query)
        
        clauses = [clause for clause in (self.where, *where) if clause is not None]
        if clauses:
            query = query.where(*clauses)
            
        if self.limit is not None:
            query = query.limit(self.limit)
        
        return query
    
    
    def _get_connection_context(self) -> AbstractContextManager[Connection]:
        return (
            nullcontext(self.connection) if isinstance(self.connection, Connection) 
            else self.connection.connect()
        )
    
    
    def _get_query_context(self, connection:Connection) -> AbstractContextManager[ColumnElement[bool] | None]:
        return (
            self.query_context(connection)
            if self.query_context is not None
            else nullcontext(None)
        )
            
    
    def prepare_dataframe(self, df: DataFrame) -> DataFrame:
        """Map columns, convert types, and configure the index."""
        if self.column_mapper is not None:
            _map_columns(df, self.column_mapper)

        if self.dtype is not None:
            df = df.astype(self.dtype) # type: ignore

        if self.index_col is not None:
            df = df.set_index(
                self.index_col,
                drop=self.index_drop,
            )

        return df
    
    
    def get_result_as_dataframe(self) -> DataFrame:
        """Execute the query and return a DataFrame with mapped column and index names."""
        
        with self._get_connection_context() as connection:
            
            with self._get_query_context(connection) as clause:
                
                query = self.get_query(
                    clause
                )
                    
                df = read_sql(
                    sql=query,
                    con=connection,
                )
            
        return self.prepare_dataframe(df)
    
    
    def generate_value(self) -> T | None:
        """Return the query result, applying the transformer or validating the output type."""
        result = self.get_result_as_dataframe()
        
        if self.df_func is not None:
            return self.df_func(result)
        
        if not self.output_.is_instance_of(result):
            raise TypeError(
                f"{type(self).__name__} produced an incompatible "
                f"output value of type {type(result).__name__}."
            )
        
        return cast(T, result)
    
