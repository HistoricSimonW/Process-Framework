from dataclasses import dataclass
from itertools import batched
from typing import Iterable

from process_framework import Step, resolve
from process_framework.steps.composition.core import HasInput
from process_framework.steps.elasticsearch.composition import (
    HasElasticsearch,
    HasElasticsearchIndex,
)

@dataclass(kw_only=True)
class DeleteByTerms(HasElasticsearchIndex, HasElasticsearch, HasInput[Iterable], Step):
    """ delete docs matching a `terms` query on values in the `subject` """
    field:str
    batch_size:int = 200


    def do(self):
        terms = resolve(self.input_)
        batches = batched(terms, self.batch_size)
        self._info('performing `delete_by_query` with a terms query')
        for i, batch in enumerate(batches):
            result = self.elasticsearch.delete_by_query(
                index=resolve(self.index),
                query={
                    'terms':{
                        self.field:list(batch)
                    }
                }
            )
            self._info(f'{i}:{result}')
            
    
    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.indices.exists(index=resolve(self.index))