from process_framework import Step, Reference
from pandas import Series
from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Any, Tuple
from logging import info
from typing import Iterable

# TODO: Pull this up to Framework
class DeleteById(Step):
    """ delete docs with `_ids` passed as `subject` from the specified `index` using the elasticsearch `bulk` helper"""
    def __init__(self, subject:Reference[Iterable], elasticsearch:Elasticsearch, index:str, *, assign_result:Reference[Any]|None=None, assert_index_exists:bool=True):
        super().__init__()
        self.subject = subject
        self.elasticsearch = elasticsearch
        self.index = index
        self.assign_result = assign_result
        self.assert_index_exists = assert_index_exists


    def do(self):
        _ids = self.subject.get_value()
        assert isinstance(_ids, Iterable)
        _ids = list(_ids)

        result = bulk(
            client=self.elasticsearch,
            index=self.index,
            actions=(
                {
                    '_op_type': 'delete',
                    '_id': _id,
                }
                for _id in _ids
            )
        )

        if not isinstance(result, list):
            result = [result]

        if self.assign_result:
            self.assign_result.set(result)

