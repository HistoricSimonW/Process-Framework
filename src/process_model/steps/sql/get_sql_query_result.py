from ..assigning_step import AssigningStep
from ...references.reference import Reference
from pandas import DataFrame, Series
from abc import ABC, abstractmethod
import pandas as pd
import sqlalchemy as sql; from sqlalchemy import Select, MetaData, Engine, TextClause, URL

class GetSqlQueryResult[T](AssigningStep[T], ABC):
    """ get the result of a sql query and assign it to `assign_to` """

    def __init__(self, assign_to:Reference[T], url_args:dict):
        super().__init__(assign_to)
        self.url_args = url_args


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
    

    def get_connection(self) -> Engine:
        return sql.create_engine(self.get_url())
    

    def get_url(self) -> URL:
        return URL.create(**self.url_args)
    

    def get_query_result(self, query:Select) -> DataFrame:
        return pd.read_sql(query, self.get_connection())


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