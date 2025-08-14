from ...references.reference import Reference
from ..assigning_step import AssigningStep
from pandas import DataFrame, Series
from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import scan
from typing import Iterable, Any, cast
from itertools import islice

DEFAULT_FILTER_PATH = 'index,took,hits.hits._id,hits.hits._source,_scroll_id,_shards'

class ScanToDataFrame[T:(Series, DataFrame)](AssigningStep[T]):
    """ assign the result of an ElasticSearch index scan to a context """
    def __init__(self, assign_to:Reference[T], elasticsearch:Elasticsearch, index:str, source:str|list[str]|bool|None, query:dict[str, Any]|None=None, dtypes:dict[str, Any]|None=None, keep_columns:list[str]|None=None, *, limit:int|None=None, size:int=1000, filter_path:str|None=DEFAULT_FILTER_PATH):
        super().__init__(assign_to)
        self.elasticsearch = elasticsearch
        self.index = index
        self.query = query
        self.size = size
        self.source = source
        self.dtypes = dtypes
        self.keep_columns = keep_columns
        self.filter_path = filter_path
        self.limit = limit
       

    def scan(self) -> Iterable[dict]:
        return scan(
            client=self.elasticsearch,
            query=self.query,
            index=self.index,
            size=self.size,
            source=self.source,
            filter_path=self.filter_path
        )
        

    @staticmethod
    def hits_to_dataframe(hits:Iterable[dict], dtypes:dict[str,Any]|None=None, columns:list[str]|None=None, limit:int|None=None):
        # build an `_id`-indexed dataframe from the `hits` iterator
        df:DataFrame = DataFrame.from_records(
            islice(hits, limit), 
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

            col_df = DataFrame.from_records(df[col].values, df.index)
            col_df = col_df[col_df.columns.difference(df.columns)]
            df = df.join(col_df, how='left').drop(col, axis=1)

        # if `columns` has been passed as a list, keep only those columns
        if isinstance(columns, list):
            df = df[df.columns.intersection(columns)]
        
        # apply `dtypes` to columns that exist in the DataFrame
        if isinstance(dtypes, dict):
            df = df.astype({k:v for k, v in dtypes.items() if k in df.columns})

        return df

    def transform_result(self, result:DataFrame) -> T:
        # this needs overwriting if the default cases (DataFrame, Series and single-element 'field') are not true
        assert isinstance(result, DataFrame), "expected result to be a DataFrame"

        # if we're assigning a DataFrame, return the result
        if self.assign_to._type is DataFrame:
            return cast(T, result)

        # we need a bit more logic to handle Series
        
        # if we're assigning to something other than a Series, we've done something wrong
        if self.assign_to._type is not Series:
            raise TypeError(f"`assign_to._type` should be in (Series, DataFrame), got `{self.assign_to._type}`")
        
        # if the result is empty, early escape
        if result.empty:
            return cast(T, Series())
                
        if isinstance(self.keep_columns, list) and len(self.keep_columns) == 1:
            column = self.keep_columns[0]
        elif isinstance(self.source, list) and len(self.source) == 1:
            column = self.source[0]
        elif isinstance(self.source, str) and ',' not in self.source and '*' not in self.source:
            column = self.source
        else:
            raise ValueError(f"could not infer single column from `keep_columns`:{self.keep_columns} or `source`:{self.source}")
        
        assert isinstance(column, str)
        
        return cast(T, result[column])

    def handle_empty_result(self, result:DataFrame) -> DataFrame:
        if len(result) > 0:
            return result
        columns = [self.source] if isinstance(self.source, str) else self.source if isinstance(self.source, list) else None
        result = DataFrame([], columns=columns)
        result.index.name = '_id'
        return result


    def generate(self) -> T:
        hits = self.scan()
        result = ScanToDataFrame.hits_to_dataframe(hits, self.dtypes, self.keep_columns, self.limit)
        result = self.handle_empty_result(result)
        return self.transform_result(result)