from ..assigning_step import AssigningStep
from ...references.reference import Reference
from pandas import DataFrame, Series
from abc import ABC, abstractmethod
import pandas as pd
import sqlalchemy as sql; from sqlalchemy import Select, MetaData, Engine, TextClause, URL, create_engine, text

class GetSqlQueryResult[T](AssigningStep[T], ABC):
    """ get the result of a sql query and assign it to `assign_to` """

    def __init__(self, assign_to:Reference[T], engine:Engine|None=None, url_create_kwargs:dict|None=None):
        super().__init__(assign_to)
        self.engine = GetSqlQueryResult.__handle_engine_init_args__(engine, url_create_kwargs)
        

    @staticmethod
    def __handle_engine_init_args__(engine:Engine|None, url_create_kwargs:dict|None):
        """ try to get an engine from the relevant constructor args
            only one arg should be received
            if `engine` is available, return it
            if `url_create_kwargs` have been passed; construct an engine from them """
        has_engine = isinstance(engine, Engine)
        has_url_args = isinstance(url_create_kwargs, dict)
        
        if not (has_engine ^ has_url_args):
            raise Exception(f"expected exactly one of `engine` and `url_create_kwargs`, got engine:{has_engine} url_create_kwargs:{has_url_args}")

        if isinstance(engine, Engine):
            return engine
        
        if isinstance(url_create_kwargs, dict):
            url = URL.create(**url_create_kwargs)
            return create_engine(url)
        
        raise Exception("`Engine` and `dict` cases should have been handled - this shouldn't be raisable!")



    def get_metadata(self) -> MetaData:
        """ initialize an instance of `MetaData`, pass it to `populate_metadata` """
        metadata = MetaData()
        self.populate_metadata(metadata)
        return metadata
    

    def populate_metadata(self, metadata:MetaData) -> None:
        """ take an initialized `MetaData` and add tables to it """
        # needs to be overwritten if `get_query` returns an ORM `Select`, but not if it returns a `text`
        pass


    @abstractmethod
    def get_query(self, metadata:MetaData) -> Select|TextClause:
        pass


    def get_query_result(self, query:Select) -> DataFrame:
        return pd.read_sql(query, self.engine)


    def transform_result(self, result) -> T:
        f""" transform the result dataframe into a {type(T)} """
        if isinstance(result, self.assign_to._type):
            return result
        
        if len(result.columns) == 1 and self.assign_to._type == Series:
            return result[result.columns[0]]
        
        raise Exception(f"`assign_to` expects a `{self.assign_to._type}`, but `result` is {type(result)}")

    
    def generate(self) -> T:
        metadata = self.get_metadata()
        query = self.get_query(metadata)
        result = self.get_query_result(query) # type: ignore
        return self.transform_result(result)
    

class GetSqlTextQueryResult(GetSqlQueryResult):
    """ a simple implementation of GetSqlQueryResult using a text cquery type """
    def __init__(self, assign_to: Reference, query:str, engine: Engine | None = None, url_create_kwargs: dict | None = None):
        super().__init__(assign_to, engine, url_create_kwargs)
        self.query = query

    def get_query(self, metadata: MetaData) -> Select | TextClause:
        return text(self.query)

