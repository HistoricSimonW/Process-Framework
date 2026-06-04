from types import TracebackType
from typing import Any
from .assign_query_result_base import GetSqlQueryResultBase
from pandas import DataFrame, Series, Index
from abc import ABC, abstractmethod
from sqlalchemy import Select, MetaData, Table, Column, Connection, insert
from sqlalchemy.schema import CreateTable, DropTable
from itertools import batched
from contextlib import AbstractContextManager
from dataclasses import dataclass
import logging
from process_framework.steps.composition.sql import InClause, TEMP_TABLE_NAME

MAX_IN_VALUES_LEN = 10_000

@dataclass(kw_only=True)
class GetOrmQueryResult[T:(DataFrame, Series, Index)](GetSqlQueryResultBase[T], ABC):
    """ get the result of a query defined using the sqlalchemy ORM"""

    def preflight(self) -> None:
        self.metadata = self.get_metadata()
        return super().preflight()


    def get_metadata(self) -> MetaData:
        """ initialize an instance of `MetaData`, pass it to `populate_metadata`; set the `in_column` from the metadata """
        metadata = MetaData()

        # populate query-specific metadata
        self.populate_metadata(metadata)

        # if we have _ids, we need a 'temp table' definition in our metadata
        if self.modifiers is not None:
            for mod in self.modifiers:
                mod.modify_metadata(metadata)

        return metadata


    @abstractmethod
    def populate_metadata(self, metadata:MetaData) -> None:
        """ take an initialized `MetaData` and add tables to it; bind tables to the Step """
        # self.ExampleTable = Table('ExampleTable', metadata, Column('id'))
        ...


    def get_query_result(self, query: Select, connection:Connection|None=None) -> DataFrame:
        # if we have _ids, execute the query in a temp table context managed by a context manager
        md = self.metadata

        if TEMP_TABLE_NAME in md.tables and self.modifiers is not None:
            
            temp_table = md.tables[TEMP_TABLE_NAME]
            _ids = next(t for t in self.modifiers if isinstance(t, InClause))

            with self.engine.connect() as conn, IdsTempTableContext(conn, temp_table, _ids.get_in_values()):
                return super().get_query_result(query, conn)

        # if we aren't handling _ids, we don't need any special logic
        return super().get_query_result(query, connection)


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