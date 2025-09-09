from ...references.reference import Reference
from ..assigning_step import Step
from .document import Document

from pandas import Series
from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Any, Tuple
from logging import info

# https://elasticsearch-py.readthedocs.io/en/v8.2.2/helpers.html
class IndexDocuments(Step):
    """ index a `Series` of `Documents` using elasticsearch.helpers.bulk """
    def __init__(self, subject:Reference[Series], elasticsearch:Elasticsearch, index:str, pipeline:str|None=None, *, assign_result:Reference[Tuple]|None=None, assert_index_exists:bool=True,
                 raise_on_error:bool=True, raise_on_exception:bool=True, max_retries:int=0, initial_backoff:int=2, 
                 chunk_size:int=500, max_chunk_bytes:int=104857600, bulk_kwargs:dict[str, Any]|None=None):
        super().__init__()
        self.subject = subject
        self.elasticsearch = elasticsearch
        self.index = index
        self.pipeline= pipeline
        self.assert_exists=assert_index_exists
        self.assign_result = assign_result

        # initialize bulk_kwargs with the provided dict, or an empty one
        self.bulk_kwargs = bulk_kwargs if isinstance(bulk_kwargs, dict) else dict()
        
        # update bulk_kwargs with named args
        self.bulk_kwargs |= dict(
            raise_on_error=raise_on_error,
            raise_on_exception=raise_on_exception,
            max_retries=max_retries,
            initial_backoff=initial_backoff,
            chunk_size=chunk_size,
            max_chunk_bytes = max_chunk_bytes
        )


    def do(self):
        # perform assertions
        assert self.subject.has_value()
        if self.assert_exists:
            assert self.elasticsearch.indices.exists(index=self.index), f'parameter `assert_index_exists==True` but index {self.index} does not exist'

        # generate actions
        series:Series = self.subject.get_value() # type: ignore
        actions = Document.gen_bulk_index_actions(
            index=self.index,
            documents=series.values
        )

        # perform bulk
        result = bulk(
            client=self.elasticsearch,
            actions=actions,
            index=self.index,
            pipeline=self.pipeline,
            **self.bulk_kwargs
        )
        
        # I don't do anything with the result (yet)
        if self.assign_result is not None:
            self.assign_result.set(result)
            
        info(f'{result}')
