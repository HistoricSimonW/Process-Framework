from process_framework import Step, Reference
from pandas import Series
from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Any, Tuple
from logging import info
from typing import Iterable
from dataclasses import dataclass
from process_framework.steps.composition.elasticsearch.core import HasElasticsearch, HasElasticsearchIndex
from process_framework.steps.composition.core import HasInput, HasOptionalOutput
from process_framework import resolve
@dataclass(kw_only=True)
class DeleteById(HasElasticsearchIndex, HasElasticsearch, HasInput[Iterable[str]], HasOptionalOutput[Any], Step):
    
    def gen_actions(self, _ids:Iterable[str]) -> Iterable[dict]:
        """Yield bulk delete actions for the supplied document IDs."""
        for _id in _ids:
            yield {
                '_index':resolve(self.index),
                '_op_type': 'delete',
                '_id': _id,
            }


    def do(self):
        _ids = self.input_.get_value()

        assert isinstance(_ids, Iterable)

        actions = self.gen_actions(_ids)

        result = bulk(
            client=self.elasticsearch,
            index=resolve(self.index),
            actions=actions
        )

        if not isinstance(result, list):
            result = [result]

        if self.output_ is not None:
            self.output_.set_value(result)

    
    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.indices.exists(index=resolve(self.index))