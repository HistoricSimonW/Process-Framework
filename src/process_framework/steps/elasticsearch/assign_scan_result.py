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
class ScanToDataFrame[T: (DataFrame, Series, Index)](AssigningStep[T]):
    """Assign the result of an Elasticsearch scan to a process-framework Reference."""

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
    column_as_index: str | list[str] | None = None
    column_mapper: Callable[[str], str] | Mapping[str, str] | None = None
    column_as_series: str | None = None
    drop_index_column: bool = True
    dtypes: dict[str, Any] | None = None

    def scan(self) -> Iterable[dict]:
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

    @staticmethod
    def hits_to_dataframe(
        hits: Iterable[dict],
        *,
        columns: list[str] | None = None,
        limit: int | None = None,
    ) -> DataFrame:
        records = list(islice(hits, limit))

        if not records:
            return ScanToDataFrame.empty_dataframe(columns)

        df = ScanToDataFrame.records_to_dataframe(records)
        df = ScanToDataFrame.expand_hit_columns(df)
        df = ScanToDataFrame.keep_requested_columns(df, columns)

        return df

    @staticmethod
    def records_to_dataframe(records: list[dict]) -> DataFrame:
        df = DataFrame.from_records(records, columns=HIT_COLUMNS)

        if "_id" in df.columns:
            df = df.set_index("_id", drop=True)
            df.index.name = "_id"

        return df

    @staticmethod
    def expand_hit_columns(df: DataFrame) -> DataFrame:
        for col in ("_source", "_fields"):
            df = ScanToDataFrame.expand_mapping_column(df, col)

        return df.drop(columns=["_index"], errors="ignore")

    @staticmethod
    def expand_mapping_column(df: DataFrame, col: str) -> DataFrame:
        if col not in df.columns:
            return df

        if df[col].isna().all():
            return df.drop(columns=[col])

        expanded = DataFrame.from_records(df[col].values, index=df.index)

        # Do not overwrite columns that already exist.
        expanded = expanded[expanded.columns.difference(df.columns)]

        return df.join(expanded, how="left").drop(columns=[col])

    @staticmethod
    def keep_requested_columns(df: DataFrame, columns: list[str] | None) -> DataFrame:
        if columns is None:
            return df

        # Preserve requested order and include missing requested columns.
        for col in columns:
            if col not in df.columns or col not in df.index:
                df[col] = NA

        return df[columns]

    @staticmethod
    def empty_dataframe(columns: list[str] | None = None) -> DataFrame:
        df = DataFrame(columns=columns)
        df.index.name = "_id"
        return df

    def transform_result(self, result: DataFrame) -> T:
        assert isinstance(result, DataFrame), "expected result to be a DataFrame"

        result = self.rename_columns(result)
        result = self.set_result_index(result)
        result = self.apply_dtypes(result)
        result = self.on_transform_result(result)

        return self.cast_result_to_output_type(result)

    def rename_columns(self, result: DataFrame) -> DataFrame:
        if self.column_mapper is None:
            return result

        return result.rename(self.column_mapper, axis=1)

    def set_result_index(self, result: DataFrame) -> DataFrame:
        if self.column_as_index is None:
            return result

        return result.reset_index().set_index(self.column_as_index, drop=self.drop_index_column)

    def apply_dtypes(self, result: DataFrame) -> DataFrame:
        if self.dtypes is None:
            return result

        applicable_dtypes = {
            col: dtype
            for col, dtype in self.dtypes.items()
            if col in result.columns
        }

        if not applicable_dtypes:
            return result

        return result.astype(applicable_dtypes)

    def on_transform_result(self, result: DataFrame) -> DataFrame:
        """Override to modify the query result before it is cast to the output type."""
        return result

    def cast_result_to_output_type(self, result: DataFrame) -> T:
        output_type = self.output_.get_type()

        if output_type == DataFrame:
            return cast(T, result)

        if output_type == Index:
            if self.result_has_requested_index(result):
                return cast(T, result.index)

            raise TypeError(
                f"`output_` expects an Index, but `column_as_index={self.column_as_index!r}` "
                f"does not match result index names {result.index.names!r}."
            )

        if output_type == Series:
            return self.cast_result_to_series(result)

        raise TypeError(f"`output_` expects {output_type}, but result is a DataFrame.")

    def result_has_requested_index(self, result: DataFrame) -> bool:
        if isinstance(self.column_as_index, str):
            return self.column_as_index in result.index.names

        if isinstance(self.column_as_index, list):
            return all(col in result.index.names for col in self.column_as_index)

        return False

    def cast_result_to_series(self, result: DataFrame) -> T:
        if self.column_as_series is None:
            raise TypeError("`column_as_series` must be set when output type is Series.")

        if self.column_as_series not in result.columns:
            raise KeyError(
                f"`column_as_series={self.column_as_series!r}` is not in result columns: "
                f"{list(result.columns)!r}"
            )

        return cast(T, result[self.column_as_series])

    def generate_value(self) -> T:
        result = self.hits_to_dataframe(
            self.scan(),
            columns=self.keep_columns,
            limit=self.limit,
        )

        return self.transform_result(result)

    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.indices.exists(index=self.index)