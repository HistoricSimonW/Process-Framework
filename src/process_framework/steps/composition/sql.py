from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from dataclasses import KW_ONLY, dataclass
from itertools import batched
import logging
from types import TracebackType
from typing import Any, Iterable

import pandas as pd
from pandas import DataFrame

from sqlalchemy import (
    Column,
    Connection,
    Engine,
    MetaData,
    Select,
    Table,
    insert,
    select,
    text,
)
from sqlalchemy.schema import CreateTable, DropTable

from ...references.composition.core import IGettable
from .core import StepMixin


@dataclass(kw_only=True)
class WithSqlEngine(StepMixin):
    """mixin providing SQL Engine and connectivity check."""
    engine:Engine

    def preflight(self) -> None:
        self.engine.connect()
        super().preflight()


@dataclass(kw_only=True)
class ProvidesQueryResults(WithSqlEngine, ABC):
    """mixin wrapping `pd.read_sql` and injecting a kwargs."""
    read_sql_kwargs:dict|None=None

    def get_query_result(self, query:Select, connection:Connection|None=None) -> DataFrame:
        
        if connection is None:
            with self.engine.connect() as con:
                return self.get_query_result(query, con)
            
        kwargs = self.read_sql_kwargs or dict()

        return pd.read_sql(
            sql=query,
            con=connection,
            **kwargs
        )


@dataclass
class OrmQueryModifierBase(ABC):
    def modify_metadata(self, metadata:MetaData) -> None:
        ...

    @abstractmethod
    def modify_query(self, metadata:MetaData|None, query:Select) -> Select:
        ...


@dataclass
class LimitClause(OrmQueryModifierBase):
    count:int|None

    def modify_query(self, metadata:MetaData|None, query:Select):
        if self.count is None:
            return query
        
        return query.limit(self.count)


@dataclass
class IdsTempTableContext(AbstractContextManager):
    """ context manager for temp-table using queries
        this ensures the temp table is cleaned up, even if the code within this context throws an error
        of course, temp tables are dropped when the connection closes; this is intended to make testing easier"""
    conn:Connection
    table:Table
    _ids:list

    def __enter__(self) -> Any:
        self.conn.execute(CreateTable(self.table))
        for batch in batched(self._ids, 1000):
            self.conn.execute(insert(self.table).values([(t,) for t in batch]))

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> None:
        self.conn.execute(DropTable(self.table))


TEMP_TABLE_NAME = '#TEMP_IDS'
TEMP_TABLE_COLUMN = '_id'

@dataclass
class InClause(OrmQueryModifierBase):
    values:IGettable[Iterable]|list
    in_table:str
    in_column:str
    values_type:Any
    
    def get_in_values(self) -> list:

        if isinstance(self.values, list):
            return self.values
        
        if isinstance(self.values, IGettable):
            return list(self.values.get_value())

        raise ValueError()
    

    def modify_metadata(self, metadata:MetaData) -> None:
        _ = Table(
            TEMP_TABLE_NAME, metadata,
            Column(TEMP_TABLE_COLUMN, self.values_type)    # id values in the temp table should be of the same type as the 'in_column'
        )
        logging.info(f'{self} has created a temp table for _ids {TEMP_TABLE_NAME}')
    

    def modify_query(self, metadata:MetaData|None, query:Select):
        assert metadata is not None
        in_table = metadata.tables[self.in_table]
        temp_table = metadata.tables[TEMP_TABLE_NAME]

        return query.join(
            temp_table, in_table.c[self.in_column] == temp_table.c[TEMP_TABLE_COLUMN]
        )
    
    
@dataclass(kw_only=True)
class BuildsQueryBase(ABC):
    modifiers:list[OrmQueryModifierBase]
    @abstractmethod
    def get_query(self) -> Select:
        ...

    def modify_query(self, step, query:Select) -> Select:
        try:
            metadata:MetaData|None = self.__getattribute__('metadata')
        except:
            metadata = None
            
        if self.modifiers is not None:
            for modifier in self.modifiers:
                query = modifier.modify_query(metadata, query)
        return query
    
    def get_modified_query(self, step) -> Select:
        query = self.get_query()
        return self.modify_query(step, query)
    

class BuildsOrmQuery(BuildsQueryBase, ABC):
    ...


@dataclass(kw_only=True)
class BuildsTextQuery(BuildsQueryBase):
    query:str
    def get_query(self):
        return select(text(self.query))
    

# class GetTextQueryResult(IGetTextQuery, IGetQueryResult):
#     ...

# from pandas import DataFrame, Series
# from ..step import AssigningStep
# from typing import cast

# @dataclass
# class AssignSqlQueryResult[T:(DataFrame, Series)](IGetQuery, IGetQueryResult, AssigningStep[T], ABC):

#     def on_transform_result(self, result:T) -> T:
#         return result

#     def transform_result(self, result:DataFrame) -> T:
#         if self.output_.can_set(result):
#             typed = cast(T, result)
#             return self.on_transform_result(typed)
        
#         raise TypeError(
#             f"can't assign {type(result).__name__} to {self.output_.get_type().__name__}"
#         )

#     def generate_value(self) -> T | None:
#         query = self.get_modified_query()
#         result = self.get_query_result(query)
#         return self.transform_result(result)
    

# class AssignTextQueryResult(AssignSqlQueryResult[DataFrame], IGetTextQuery):
#     ...
        