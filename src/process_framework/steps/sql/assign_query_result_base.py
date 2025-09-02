from ...references.reference import Reference
from ..assigning_step import AssigningStep
from abc import ABC, abstractmethod
import pandas as pd; from pandas import DataFrame, Series
from sqlalchemy import Select, Engine, URL, create_engine, Connection
from typing import Any, Mapping, Callable

class GetSqlQueryResultBase[T:(DataFrame, Series)](AssigningStep[T], ABC):
    """ base class for Steps that assign the result of Sql queries to `assign_to`"""

    def __init__(self, assign_to:Reference[T], *, 
                 engine:Engine|None=None, url_create_kwargs:dict|None=None, column_mapper:dict|Mapping|Callable[[str], str]|None=None, index:str|Any|None=None):
        super().__init__(assign_to)
        self.engine = GetSqlQueryResultBase.__handle_engine_init_args__(engine, url_create_kwargs)
        self.column_mapper = column_mapper
        self.index = index
        

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


    @abstractmethod
    def get_query(self) -> Select:
        ...


    def qualify_query(self, query) -> Select:
        """ apply any limit, where, in() clauses """
        return query


    def get_qualified_query(self) -> Select:
        """ get the query, modified by any qualifiers """
        query = self.get_query()
        return self.qualify_query(query)


    def get_query_result(self, query:Select, conn:Connection) -> DataFrame:
        """ get the result of the qualified query as a DataFrame """
        return pd.read_sql(query, conn)
    

    def transform_result(self, result) -> T:
        f""" transform the query result dataframe into a {type(T)} """
        if self.column_mapper is not None:
            result.columns = result.columns.map(self.column_mapper)

        if isinstance(self.index, str):
            if self.index in result.index.names:
                pass 
            elif self.index in result.columns:
                result = result.set_index(self.index)
            else:
                print(f"index `{self.index}` was not a name in `result`'s columns or index; has it been changed by `column_mapper` {self.column_mapper}?")
            
        if isinstance(result, self.assign_to._type):
            return result
        
        if len(result.columns) == 1 and self.assign_to._type == Series:
            return result[result.columns[0]]
        
        raise Exception(f"`assign_to` expects a `{self.assign_to._type}`, but `result` is {type(result)}")

    
    def generate(self) -> T:
        """ get the result of the query and transform it to a type assignable to `assign_to` """
        query = self.get_qualified_query()
        with self.engine.connect() as conn, conn.begin():
            result:DataFrame = self.get_query_result(query, conn)
        return self.transform_result(result)