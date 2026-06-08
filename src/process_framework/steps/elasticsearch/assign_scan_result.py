from dataclasses import dataclass
from itertools import islice
from typing import Any, Callable, Iterable, Mapping, cast

from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import scan
from pandas import DataFrame, Index, Series, NA

from process_framework import AssigningStep


DEFAULT_FILTER_PATH = "index,took,hits.hits._id,hits.hits._source,_scroll_id,_shards"
HIT_COLUMNS = ["_id", "_index", "_source", "_fields"]


@dataclass(kw_only=True)
class AssignScanResult[T: (DataFrame, Series, Index)](AssigningStep[T]):
    """Assign the result of an Elasticsearch scan to a typed Reference."""

    # query args
    elasticsearch: Elasticsearch
    index: str
    query: dict | None = None
    size: int | None = None
    source: str | list[str] | None = None
    filter_path: str | None = None
    limit: int | None = None

    # processing args
    keep_columns: list[str] | None = None
    column_as_index: str | list[str] | None = '_id'
    column_mapper: Callable[[str], str] | Mapping[str, str] | None = None
    column_as_series: str | None = None
    drop_index_column: bool = True
    dtypes: dict[str, Any] | None = None

    def scan(self) -> Iterable[dict]:
        """ initialize a scan; yield hits as dicts """
        args: dict[str, Any] = {
            "client": self.elasticsearch,
            "query": self.query,
            "index": self.index,
            "source": self.source,
            "filter_path": self.filter_path,
        }

        if self.size is not None:
            args["size"] = self.size

        return scan(**args)


    def hits_to_dataframe(self, hits: Iterable[dict],*,columns: list[str] | None = None, limit: int | None = None) -> DataFrame:
        """ construct a dataframe from scan hits """
        # exhaust an islice of hits into a list of records
        records = list(islice(hits, limit))

        # if records is empty, return an empty dataframe
        if not records:
            return AssignScanResult.empty_dataframe(columns)

        df = DataFrame.from_records(records, columns=HIT_COLUMNS)
        
        for col in ('_source', '_fields'):
            if col not in df.columns:
                continue
            df = self.expand_mapping_column(df, col)
                
        df = self.keep_required_columns(df, columns)

        return df


    @staticmethod
    def expand_mapping_column(df: DataFrame, col: str) -> DataFrame:
        """ try to expand 'col' of dataframe, where 'col' is a column of nested records """
        if col not in df.columns:
            return df

        if df[col].isna().all():
            return df.drop(columns=[col])

        expanded = DataFrame.from_records(df[col].values, index=df.index)

        # Do not overwrite columns that already exist.
        expanded = expanded[expanded.columns.difference(df.columns)]

        return df.join(expanded, how="left").drop(columns=[col])

    
    def keep_required_columns(self, df: DataFrame, columns: list[str] | None) -> DataFrame:
        if columns is None:
            return df

        cols = columns

        if isinstance(self.column_as_index, str):
            cols.append(self.column_as_index)

        if isinstance(self.column_as_index, list):
            cols += self.column_as_index

        # Preserve requested order and include missing requested columns.
        for col in cols:
            if col not in df.columns:
                df[col] = NA

        return df[columns]


    @staticmethod
    def empty_dataframe(columns: list[str] | None = None) -> DataFrame:
        """ construct an empty dataframe with the requested columns and '_id' as index """
        df = DataFrame(columns=columns)
        df.index.name = "_id"
        return df


    def transform_result(self, result: DataFrame) -> T:
        assert isinstance(result, DataFrame), "expected result to be a DataFrame"

        if self.column_mapper is not None:
            result = result.rename(self.column_mapper, axis=1)
        
        if self.dtypes is not None:
            result = result.astype(self.dtypes) # type:ignore

        if self.column_as_index is not None:
            result = result.set_index(self.column_as_index, drop=self.drop_index_column)

        result = self.on_transform_result(result)

        return self._cast_result_to_type(result)


    def on_transform_result(self, result: DataFrame) -> DataFrame:
        """Override to modify the query result before it is cast to the output type."""
        return result


    def _cast_result_to_type(self, result:DataFrame) -> T:
        """ handle the result DataFrame into the required output type """
        output_type = self.output_.get_type()

        if output_type == DataFrame:
            return cast(T, result)
        
        if output_type == Index and (
            (isinstance(self.column_as_index, str) and self.column_as_index in result.index.names) or
            (isinstance(self.column_as_index, list) and all(t in result.index.names for t in self.column_as_index))
        ):
            return cast(T, result.index)
        
        if output_type == Series and self.column_as_series is not None:
            if len(result.columns) == 0:
                self._warn("got result with zero columns, returning an empty Series")
                return cast(T, Series([], name=self.column_as_series))
            
            if self.column_as_series in result.columns:
                return cast(T, result[self.column_as_series])
            else:
                self._warn(f"column `{self.column_as_series}` was not a name in `result`'s columns; has it been changed by `column_mapper` {self.column_mapper}?")

        raise Exception(f"`assign_to` expects a `{output_type}`, but `result` is {type(result)}")


    def generate_value(self) -> T:
        hits = self.scan()
        df = self.hits_to_dataframe(hits, columns=self.keep_columns, limit=self.limit)
        return self.transform_result(df)


    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.indices.exists(index=self.index)