from types import TracebackType
from typing import Any, Mapping, Callable
from ...references.reference import Reference
from ...references.dataframe.reference_column import ColumnReference
from .assign_query_result_base import GetSqlQueryResultBase
from pandas import DataFrame, Series, Index
from abc import ABC, abstractmethod
from sqlalchemy import Select, MetaData, Engine, TextClause, ColumnElement, Table, Column, Connection, insert
from sqlalchemy.schema import CreateTable, DropTable
from itertools import batched
from contextlib import AbstractContextManager
from dataclasses import dataclass
import logging
from process_framework.steps.composition.sql import IAddInClause

MAX_IN_VALUES_LEN = 10_000
TEMP_TABLE_NAME = '#TEMP_IDS'
TEMP_TABLE_ID = '_id'

@dataclass
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
        for mod in self.modifiers:
            if isinstance(mod, IAddInClause):
                self._temp_table = self._get_temp_table_metadata(mod, metadata)
                logging.info(f'{self} has created a temp table for _ids {self._temp_table}')

        return metadata


    def _get_temp_table_metadata(self, in_clause:IAddInClause, metadata):
        # because this is claled within `get_metadata`, `in_column` must be set; we rely on `in_column` to define the type of _id
        return Table(
            TEMP_TABLE_NAME, metadata,
            Column(TEMP_TABLE_ID, in_clause.in_values_type)    # id values in the temp table should be of the same type as the 'in_column'
        )


    @abstractmethod
    def populate_metadata(self, metadata:MetaData) -> None:
        """ take an initialized `MetaData` and add tables to it; bind tables to the Step """
        # self.ExampleTable = Table('ExampleTable', metadata, Column('id'))
        ...


    def get_query_result(self, query: Select) -> DataFrame:
        # if we have _ids, execute the query in a temp table context managed by a context manager
        
        _ids = next((m for m in self.modifiers if isinstance(m, IAddInClause)), None)

        if _ids is not None:
            temp_table = self._temp_table

            with IdsTempTableContext(self.engine.connect(), temp_table, _ids.get_in_values()):
                return super().get_query_result(query)

        # if we aren't handling _ids, we don't need any special logic
        return super().get_query_result(query)


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