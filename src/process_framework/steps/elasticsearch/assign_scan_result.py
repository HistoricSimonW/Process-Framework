from process_framework import AssigningStep, Reference
from pandas import DataFrame, Series, Index
from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import scan
from typing import Iterable, Any, cast, Callable, Mapping
from itertools import islice
from dataclasses import dataclass
DEFAULT_FILTER_PATH = 'index,took,hits.hits._id,hits.hits._source,_scroll_id,_shards'

@dataclass(kw_only=True)
class ScanToDataFrame[T:(DataFrame, Series, Index)](AssigningStep[T]):
    """ assign the result of an ElasticSearch index scan to a context """
    # query args
    elasticsearch:Elasticsearch
    index:str
    query:dict|None=None
    size:int|None=None
    source:str|list[str]|None=None
    filter_path:str|None=None
    limit:int|None=None

    # processing args
    keep_columns:list[str]|None=None
    column_as_index:str|list[str]|None=None
    column_mapper:Callable[[str], str]|Mapping[str,str]|None=None
    column_as_series:str|None=None
    drop_index_column:bool=True
    append_index_column:bool=True
    dtypes:dict[str, Any]|None=None
       

    def scan(self) -> Iterable[dict]:
        args:dict = dict(
            client=self.elasticsearch,
            query=self.query,
            index=self.index,
            source=self.source,
            filter_path=self.filter_path
        )

        if self.size is not None:
            args['size'] = self.size
            
        return scan(**args)
        

    @staticmethod
    def hits_to_dataframe(hits:Iterable[dict], dtypes:dict[str,Any]|None=None, columns:list[str]|None=None, limit:int|None=None):
        # build an `_id`-indexed dataframe from the `hits` iterator
        records = list(islice(hits, limit))
        
        if not records:
            return DataFrame()
        
        df:DataFrame = DataFrame.from_records(
            records,  # type: ignore
            index='_id', 
            columns=['_id', '_index', '_source', '_fields']
        )

        if df.empty:
            print('`df` is empty, returning an empty DataFrame')
            return DataFrame()

        # unnest `_source` and `fields`, drop (plus column "index") if NA
        for col in ('_source', '_fields', '_index'):

            if (col not in df.columns):
                continue
            
            if (df[col].isna().all()):
                df = df.drop(col, axis=1)
                continue

            col_df = DataFrame.from_records(df[col].values, df.index) # type: ignore
            col_df = col_df[col_df.columns.difference(df.columns)]
            df = df.join(col_df, how='left').drop(col, axis=1)

        # if `columns` has been passed as a list, keep only those columns
        if isinstance(columns, list):
            for col in columns:
                if col not in columns:
                    df[col] = None

            df = df[df.columns.intersection(columns)]
        
        df = df.reset_index()

        # apply `dtypes` to columns that exist in the DataFrame
        if isinstance(dtypes, dict):
            df = df.astype({k:v for k, v in dtypes.items() if k in df.columns})

        return df


    def transform_result(self, result:DataFrame) -> T:
        # this needs overwriting if the default cases (DataFrame, Series and single-element 'field') are not true
        assert isinstance(result, DataFrame), "expected result to be a DataFrame"

        if self.column_mapper is not None:
            result = result.rename(
                self.column_mapper, 
                axis=1
            )
        
        if self.column_as_index is not None:
            result = result.set_index(
                self.column_as_index, 
                drop=self.drop_index_column,
                append=self.append_index_column
            )

        if self.dtypes is not None:
            result = result.astype(self.dtypes)

        result = self.on_transform_result(result)

        return self._cast_result_to_type(result)


    def on_transform_result(self, result:DataFrame) -> DataFrame:
        """ overwrite this to modify the query result DataFrame before it is cast to the result type
            this is the place to do any bespoke text transformations or other conditional logic """
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


    def handle_empty_result(self, result:DataFrame) -> DataFrame:
        if len(result) > 0:
            return result
        columns = [self.source] if isinstance(self.source, str) else self.source if isinstance(self.source, list) else None
        result = DataFrame([], columns=columns)
        result.index.name = '_id'
        return result


    def generate_value(self) -> T:
        hits = self.scan()
        result = ScanToDataFrame.hits_to_dataframe(hits, self.dtypes, self.keep_columns, self.limit)
        result = self.handle_empty_result(result)
        return self.transform_result(result)
    

    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.indices.exists(index=self.index)