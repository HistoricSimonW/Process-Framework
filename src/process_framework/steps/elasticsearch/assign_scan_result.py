from collections.abc import Generator
from contextlib import closing
from dataclasses import dataclass
from itertools import islice
from typing import Any, Callable, Sequence, cast

from elasticsearch.helpers import scan

from process_framework import AssigningStep, resolve
from process_framework.steps.composition.elasticsearch.core import (
    HasElasticsearchIndex,
)

from .transformation import Hit, HitsTransformer
from ..composition.elasticsearch.query import Query

@dataclass(kw_only=True)
class GetScanResult[T](AssigningStep[T], HasElasticsearchIndex):
    """Scan an Elasticsearch index and assign its transformed results."""
    hits_func:HitsTransformer[T]
    query:Query|dict[str, Any]|Callable[[], dict[str, Any]]|None=None
    source:str|Sequence[str]|bool|None=None
    source_includes:str|Sequence[str]|None=None
    source_excludes:str|Sequence[str]|None=None
    fields:Sequence[str]|None=None
    filter_path:str|None=None
    size:int=1000
    scroll:str='5m'
    limit:int|None=None
    
    def get_query(self) -> dict[str, Any]:
        """Resolve the Elasticsearch query, defaulting to match_all."""
        query = resolve(self.query)
        
        if isinstance(query, dict):
            return query
        
        if isinstance(query, Query):
            return query.get_query()
        
        if callable(query):
            return query()
        
        if query is None:
            query = {'match_all':{}}
            
        raise ValueError()
    
    
    def gen_hits(self) -> Generator[Hit]:
        """Yield matching hits up to the optional limit."""
        hits = cast(
            Generator[Hit], 
            scan(
                client=self.elasticsearch,
                index=resolve(self.index),
                source=self.source,
                source_includes=self.source_includes,
                source_excludes=self.source_excludes,
                fields=self.fields,
                filter_path=self.filter_path,
                size=self.size,
                scroll=self.scroll,
                query={'query':self.get_query()}
            )
        )
        
        with closing(hits):
            yield from islice(hits, self.limit)
    
    
    def generate_value(self) -> T:
        """Transform matching hits into the output value."""
        with closing(self.gen_hits()) as hits:
            return self.hits_func(hits)
        