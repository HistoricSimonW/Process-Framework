from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable

import logging

from elasticsearch.helpers import BulkIndexError, bulk

from process_framework import Step
from process_framework.steps.composition.core import (
    HasInput,
    HasOptionalOutput,
)
from process_framework.steps.composition.elasticsearch.actions import IndexAction
from process_framework.steps.composition.elasticsearch.core import (
    HasElasticsearch,
    HasElasticsearchIndex,
)
from process_framework.steps.composition.elasticsearch.document import DocumentBase


@contextmanager
def suppress_logging(names: Iterable[str], level: int = logging.WARNING):
    """ a context manager to temporarily suppress loggers identified by names """
    loggers = [logging.getLogger(name) for name in names]
    previous = [(logger, logger.level, logger.disabled) for logger in loggers]

    try:
        for logger in loggers:
            logger.setLevel(level)
        yield
    finally:
        for logger, old_level, old_disabled in previous:
            logger.setLevel(old_level)
            logger.disabled = old_disabled


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
        self._info(f"Indexing documents into {self.index} with pipeline={self.pipeline!r}")
        try:
            with suppress_logging(
                    [
                        "elastic_transport.transport",
                        "urllib3.connectionpool",
                    ],
                    level=logging.WARNING,
            ):
                result = bulk(
                    self.elasticsearch,
                    actions=IndexAction.from_documents(docs, self.index),
                    pipeline=self.pipeline,
                    **kwargs
                )

        except BulkIndexError as e:
            for error in e.errors:
                self._error(f'{error}')
            raise e

        if self.output_ is not None:
            self.output_.set_value(result)