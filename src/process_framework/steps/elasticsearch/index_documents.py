from process_framework import Step, Reference
from pandas import Series
from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from typing import Any, Tuple
from logging import info
from typing import NamedTuple, Iterable
from dataclasses import dataclass
from process_framework.steps import Step
from process_framework.steps.composition.core import HasInput, HasOptionalOutput
from process_framework.steps.composition.elasticsearch.core import HasElasticsearch, HasElasticsearchIndex
from process_framework.steps.composition.elasticsearch.document import DocumentBase

# https://elasticsearch-py.readthedocs.io/en/v8.2.2/helpers.html
@dataclass(kw_only=True)
class IndexDocuments(HasElasticsearchIndex, HasElasticsearch, HasInput[Iterable[DocumentBase]], HasOptionalOutput[Any], Step):
    """ index a `Series` of `Documents` using elasticsearch.helpers.bulk """
    pipeline:str|None=None 
    
    assert_index_exists:bool=True

    raise_on_error:bool=True
    raise_on_exception:bool=True 
    max_retries:int=0
    initial_backoff:int=2 
    chunk_size:int=500
    max_chunk_bytes:int=104857600 
    bulk_kwargs:dict[str, Any]|None=None
        
    def get_bulk_kwargs(self) -> dict:
        
        kwargs = self.bulk_kwargs or dict()

        # update bulk_kwargs with named args
        kwargs.update(dict(
            raise_on_error=self.raise_on_error,
            raise_on_exception=self.raise_on_exception,
            max_retries=self.max_retries,
            initial_backoff=self.initial_backoff,
            chunk_size=self.chunk_size,
            max_chunk_bytes = self.max_chunk_bytes
        ))

        return kwargs
    

    def do(self):
        docs = self.input_.get_value()
        kwargs = self.get_bulk_kwargs()

        try:
            result = bulk(
                self.elasticsearch,
                actions=DocumentBase.gen_bulk_index_actions(self.index, docs),
                index=self.index,
                pipeline=self.pipeline,
                **kwargs
            )

        except BulkIndexError as e:
            for error in e.errors:
                self._error(f'{error}')
            raise e

        if self.output_ is not None:
            self.output_.set_value(result)