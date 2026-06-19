from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable

import logging

from elasticsearch.helpers import BulkIndexError, bulk, streaming_bulk

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


from process_framework.context_managers.logging import suppress_logging


# https://elasticsearch-py.readthedocs.io/en/v8.2.2/helpers.html
@dataclass(kw_only=True)
class IndexDocuments(HasElasticsearchIndex, HasElasticsearch, HasInput[Iterable[DocumentBase]], HasOptionalOutput[Any], Step):
    """ index a `Series` of `Documents` using elasticsearch.helpers.bulk """
    pipeline:str|None=None 
    
    assert_index_exists:bool=True

    raise_on_error: bool = True
    raise_on_exception: bool = True
    max_retries: int = 3
    initial_backoff: int = 2
    max_backoff: int = 60
    retry_on_status: tuple[int, ...] = (429, 502, 503, 504)

    chunk_size: int = 500
    max_chunk_bytes: int = 104_857_600
    bulk_kwargs: dict[str, Any] | None = None
        
    def get_bulk_kwargs(self) -> dict:
        
        kwargs = self.bulk_kwargs or dict()

        # update bulk_kwargs with named args
        kwargs.update(
            raise_on_error=self.raise_on_error,
            raise_on_exception=self.raise_on_exception,
            max_retries=self.max_retries,
            initial_backoff=self.initial_backoff,
            max_backoff=self.max_backoff,
            retry_on_status=self.retry_on_status,
            chunk_size=self.chunk_size,
            max_chunk_bytes=self.max_chunk_bytes,
        )

        return kwargs
    

    def do(self):
        docs = self.input_.get_value()
        actions = IndexAction.from_documents(docs, self.index)
        kwargs = self.get_bulk_kwargs()
        self._info(f"Indexing documents into {self.index} with pipeline={self.pipeline!r}")
        errors = []
        indexed = 0
        
        try:
            with suppress_logging(
                    [
                        "elastic_transport.transport",
                        "urllib3.connectionpool",
                    ],
                    level=logging.WARNING,
            ):
                for ok, item in streaming_bulk(
                    client=self.elasticsearch,
                    actions=actions,
                    pipeline=self.pipeline,
                    **kwargs
                ):
                    if ok:
                        indexed += 1
                    else:
                        errors.append(item)
                        self._error(str(item))

        except BulkIndexError as e:
            for error in e.errors:
                self._error(str(error))
            raise e

        result = {
            'indexed':indexed,
            'errors':errors
        }
        
        if self.output_ is not None:
            self.output_.set_value(result)