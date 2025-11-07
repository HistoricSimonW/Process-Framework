from process_framework import Step, Reference
from elasticsearch.client import Elasticsearch
from typing import Any
from typing import Iterable
from itertools import batched
import logging

class DeleteByTerms(Step):
    """ delete docs matching a `terms` query on values in the `subject` """
    def __init__(self, subject:Reference[Iterable], elasticsearch:Elasticsearch, index:str, field:str, *, assign_result:Reference[Any]|None=None, assert_index_exists:bool=True):
        super().__init__()
        self.subject = subject
        self.elasticsearch = elasticsearch
        self.index = index
        self.field = field
        self.assign_result = assign_result
        self.assert_index_exists = assert_index_exists
        self.batch_size = 200


    def do(self):
        terms = self.subject.get_value()
        batches = batched(terms, self.batch_size)
        for batch in batches:
            result = self.elasticsearch.delete_by_query(
                index=self.index,
                query={
                    'terms':{
                        self.field:list(batch)
                    }
                }
            )
            logging.info(f'{result}')
            
    
        
    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.indices.exists(index=self.index)