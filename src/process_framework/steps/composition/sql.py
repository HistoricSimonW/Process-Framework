from .core import StepMixin
from sqlalchemy import Engine, Select, text, select
from dataclasses import dataclass, KW_ONLY
from abc import ABC, abstractmethod
from ...references.composition.core import IGettable
from typing import Iterable, Mapping, Callable
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
    @abstractmethod
    def modify_query(self, query:Select) -> Select:
        ...


@dataclass
class ILimitQuery(IModifyOrmQuery):
    count:int|None

    def modify_query(self, query):
        if self.count is None:
            return query
        
        return query.limit(self.count)
    

@dataclass
class IAddInClause(IModifyOrmQuery):
    in_values:IGettable[Iterable]|None
    in_column:str|None

    def get_in_values(self) -> list|None:
        ref = self.in_values
        if ref is None:
            return None
        
        if not ref.has_value():
            return None
        
        values = list(ref.get_value())
        return values
    
    def modify_query(self, query):
        values = self.get_in_values()
        if values is not None and self.in_column is not None:
            query = query.where(text(f'{self.in_column} IN ({','.join(values)})'))
        return query
    
    
@dataclass
class IGetQuery(ABC):
    modifiers:list[IModifyOrmQuery]
    @abstractmethod
    def get_query(self) -> Select:
        ...

    def modify_query(self, query:Select) -> Select:
        if self.modifiers is not None:
            for modifier in self.modifiers:
                query = modifier.modify_query(query)
        return query
    
    def get_modified_query(self) -> Select:
        query = self.get_query()
        return self.modify_query(query)
    

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
        