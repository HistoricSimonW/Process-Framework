from types import TracebackType
from typing import Any, Mapping, Callable
from ...references.reference import Reference
from ...references.reference_dataframe_column import ColumnReference
from .assign_query_result_base import GetSqlQueryResultBase
from pandas import DataFrame, Series
from abc import ABC, abstractmethod
from sqlalchemy import Select, MetaData, Engine, TextClause, ColumnElement, Table, Column, Connection, insert
from sqlalchemy.schema import CreateTable, DropTable
from itertools import batched
from contextlib import AbstractContextManager
from dataclasses import dataclass
from process_framework.references.reference import _repr

MAX_IN_VALUES_LEN = 10_000
TEMP_TABLE_NAME = '#TEMP_IDS'
TEMP_TABLE_ID = '_id'

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


    def has_ids(self) -> bool:
        """ return `True` if this has a reference to `_ids`; don't test if the `_ids` are valid or not;
            we don't need to test the type here; that's been done by Pydantic """
        return self._ids is not None


    def get_ids(self) -> list|None:
        """ handle _ids from list or Reference[list] or Reference[Series] or None to list or None """
        _ids = self._ids
        if _ids is None or isinstance(_ids, list):
            return _ids

        if isinstance(_ids, Reference) and _ids.is_instance_of(list):
            value = _ids.get_value()
            assert isinstance(value, list)# or value is None
            return value

        if (isinstance(_ids, Reference) and _ids.is_instance_of(Series)) or isinstance(_ids, ColumnReference):
            value = _ids.get_value()
            if isinstance(value, Series):
                return value.to_list()
            return None

        raise Exception()


    def get_metadata(self) -> MetaData:
        """ initialize an instance of `MetaData`, pass it to `populate_metadata`; set the `in_column` from the metadata """
        metadata = MetaData()

        # populate query-specific metadata
        self.populate_metadata(metadata)

        # set `in_column` once `metadata` is populated
        self.in_column = self.get_in_column()

        # if we have _ids, we need a 'temp table' definition in our metadata
        if self.has_ids():
            self._temp_table = self._get_temp_table_metadata(metadata)
            print(self._temp_table)

        return metadata


    def _get_temp_table_metadata(self, metadata):
        # because this is claled within `get_metadata`, `in_column` must be set; we rely on `in_column` to define the type of _id
        return Table(
            TEMP_TABLE_NAME, metadata,
            Column(TEMP_TABLE_ID, self.get_in_column().type)    # id values in the temp table should be of the same type as the 'in_column'
        )


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

        if self.has_ids():
            query = query.join(
                self._temp_table, self.get_in_column() == self._temp_table.c[TEMP_TABLE_ID]
            )

        return query


    def get_query_result(self, query: Select, conn: Connection) -> DataFrame:
        # if we have _ids, execute the query in a temp table context managed by a context manager
        if (_ids := self.get_ids()):

            with IdsTempTableContext(conn, self._temp_table, _ids):
                return super().get_query_result(query, conn)

        # if we aren't handling _ids, we don't need any special logic
        return super().get_query_result(query, conn)


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