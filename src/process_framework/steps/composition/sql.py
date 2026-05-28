from .core import StepMixin
from sqlalchemy import Engine, Select, text, select, MetaData
from dataclasses import dataclass, KW_ONLY
from abc import ABC, abstractmethod
from ...references.composition.core import IGettable
from typing import Iterable, Mapping, Callable, Any
from pandas import DataFrame
import pandas as pd

@dataclass
class HasSqlEngine(StepMixin):
    """mixin providing SQL Engine and connectivity check."""
    engine:Engine

    def preflight(self) -> None:
        self.engine.connect()
        super().preflight()


@dataclass
class IGetQueryResult(HasSqlEngine, ABC):
    """mixin wrapping `pd.read_sql` and injecting a kwargs."""
    _ = KW_ONLY
    read_sql_kwargs:dict|None=None

    def get_query_result(self, query:Select) -> DataFrame:
        kwargs = self.read_sql_kwargs or dict()

        return pd.read_sql(
            sql=query,
            con=self.engine,
            **kwargs
        )


@dataclass
class IModifyOrmQuery(ABC):
    def modify_metadata(self, step, metadata:MetaData) -> None:
        ...

    @abstractmethod
    def modify_query(self, step, query:Select) -> Select:
        ...


@dataclass
class ILimitQuery(IModifyOrmQuery):
    count:int|None

    def modify_query(self, step, query):
        if self.count is None:
            return query
        
        return query.limit(self.count)


from sqlalchemy import Select, MetaData, Engine, TextClause, ColumnElement, Table, Column, Connection, insert
from sqlalchemy.schema import CreateTable, DropTable
from itertools import batched
from contextlib import AbstractContextManager
from types import TracebackType

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
TEMP_TABLE_ID = '_id'


@dataclass
class IAddInClause(IModifyOrmQuery):
    in_values:IGettable[Iterable]
    in_column_getter:Callable[[MetaData], Column]
    in_values_type:Any
    
    def get_in_values(self) -> list:
        ref = self.in_values
        if ref is None:
            return []
        
        if not ref.has_value():
            return []
        
        values = list(ref.get_value())
        return values
    

    def modify_metadata(self, step, metadata: MetaData) -> None:
        step.TempTable = Table(
            TEMP_TABLE_NAME, metadata,
            Column(TEMP_TABLE_ID, self.in_values_type)    # id values in the temp table should be of the same type as the 'in_column'
        )
    

    def modify_query(self, step, query):
        in_column = self.in_column_getter(step.metadata)
        return query.join(
            step._temp_table, in_column == step._temp_table.c[TEMP_TABLE_ID]
        )
    
    
@dataclass
class IGetQuery(ABC):
    modifiers:list[IModifyOrmQuery]
    @abstractmethod
    def get_query(self) -> Select:
        ...

    def modify_query(self, step, query:Select) -> Select:
        if self.modifiers is not None:
            for modifier in self.modifiers:
                query = modifier.modify_query(step, query)
        return query
    
    def get_modified_query(self, step) -> Select:
        query = self.get_query()
        return self.modify_query(step, query)
    

@dataclass
class IGetOrmQuery(IGetQuery, ABC):
    ...


@dataclass
class IGetTextQuery(IGetQuery):
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
        