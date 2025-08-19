from ...references.reference import Reference
from .assign_query_result_base import GetSqlQueryResultBase
from pandas import DataFrame, Series
from sqlalchemy import Engine, TextClause, text
from typing import Any, Mapping, Callable

class GetTextQueryResult[T:(DataFrame, Series)](GetSqlQueryResultBase[T]):
    """ a simple implementation of GetSqlQueryResult using a text query type """

    def __init__(self, assign_to: Reference[T], query:str, *, 
                 engine: Engine | None = None, url_create_kwargs: dict | None = None, column_mapper:dict|Mapping|Callable[[str], str]|None=None, index: Any | None=None):
        super().__init__(assign_to, engine=engine, url_create_kwargs=url_create_kwargs, column_mapper=column_mapper, index=index)
        self.query = query

    def get_query(self) -> TextClause:
        """ get a sqlalchemy text clause from the provided `query` """
        return text(self.query)
    

    def transform_result(self, result) -> T:
        return super().transform_result(result)