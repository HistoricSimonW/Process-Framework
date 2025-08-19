from typing import Any, Mapping, Callable
from ...references.reference import Reference
from .assign_query_result_base import GetSqlQueryResultBase
from pandas import DataFrame, Series
from abc import ABC, abstractmethod
from sqlalchemy import Select, MetaData, Engine, TextClause, ColumnElement


class GetOrmQueryResult[T:(DataFrame, Series)](GetSqlQueryResultBase[T], ABC):
    """ get the result of a query defined using the sqlalchemy ORM"""

    def __init__(self, assign_to: Reference[T], *, engine: Engine | None = None, url_create_kwargs: dict | None = None, column_mapper:dict|Mapping|Callable[[str], str]|None=None, index: Any | None=None,
                 limit:int|None=None, _ids:list|Reference[list]|Reference[Series]|None=None, where:str|None=None):
        super().__init__(assign_to, engine=engine, url_create_kwargs=url_create_kwargs, column_mapper=column_mapper, index=index)
        # qualifiers
        self.limit:int|None=limit
        self._ids:list|Reference[list]|Reference[Series]|None = _ids
        self.where:str|None = where

        # metadata (do this here so we fail early, rather than during pipeline execution)
        self.metadata = self.get_metadata()
        self.in_column = self.get_in_column()
        

    def get_ids(self) -> list|None:
        """ handle _ids from list or Reference[list] or Reference[Series] or None to list or None """
        _ids = self._ids
        if _ids is None or isinstance(_ids, list):
            return _ids
        
        if isinstance(_ids, Reference) and _ids.is_instance_of(list):
            value = _ids.get_value(False)
            assert isinstance(value, list) or value is None
            return value
        
        if isinstance(_ids, Reference) and _ids.is_instance_of(Series):
            value = _ids.get_value(False)
            if isinstance(value, Series):
                return value.to_list()
            return None
            
        raise Exception()
    
    
    def get_metadata(self) -> MetaData:
        """ initialize an instance of `MetaData`, pass it to `populate_metadata` """
        metadata = MetaData()
        self.populate_metadata(metadata)
        return metadata
    

    @abstractmethod
    def get_in_column(self) -> ColumnElement:
        """ define the column used for 'column in(_ids)' expressions """
        ...


    @abstractmethod
    def populate_metadata(self, metadata:MetaData) -> None:
        """ take an initialized `MetaData` and add tables to it; bind tables to the Step """
        # self.ExampleTable = Table('ExampleTable', metadata, Column('id'))
        ...


    def qualify_query(self, query:Select) -> Select:
        """ apply limit, where and in() qualifiers to a query """
        if isinstance(self.limit, int):
            query = query.limit(self.limit)

        if isinstance(self.where, str):
            query = query.where(TextClause(self.where))

        _ids = self.get_ids()
        if isinstance(_ids, list) and len(_ids) < 1_000:
            query = query.where(self.in_column.in_(_ids))
        
        return query

    
    def generate(self) -> T:
        """ generate a `T` by getting a query, getting its result, and transforming it """
        query = self.get_qualified_query()
        result = self.get_query_result(query) # type: ignore
        return self.transform_result(result)