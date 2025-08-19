from ...references.reference import Reference
from .assign_query_result_base import GetSqlQueryResultBase
from pandas import DataFrame, Series
from sqlalchemy import Engine, TextClause, text


class GetTextQueryResult[T:(DataFrame, Series)](GetSqlQueryResultBase[T]):
    """ a simple implementation of GetSqlQueryResult using a text query type """
    
    def __init__(self, assign_to: Reference[T], *, 
                 query:str, engine: Engine | None = None, url_create_kwargs: dict | None = None):
        super().__init__(assign_to, engine=engine, url_create_kwargs=url_create_kwargs)
        self.query = query


    def get_query(self) -> TextClause:
        """ get a sqlalchemy text clause from the provided `query` """
        return text(self.query)