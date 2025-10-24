from ..step import Step
from elasticsearch.client import Elasticsearch
from typing import Sequence

# https://elasticsearch-py.readthedocs.io/en/v8.2.2/api.html?highlight=execute#elasticsearch.Elasticsearch.update_by_query
class UpdateByQuery(Step):
    """ perform an update-by-query operation on an index """
    # TODO : build option to pass and do a query
    def __init__(self, elasticsearch:Elasticsearch, index:str|Sequence[str], pipeline:str) -> None:
        self.elasticsearch = elasticsearch
        self.index = index
        self.pipeline = pipeline

    
    def do(self):
        self.elasticsearch.update_by_query(
            index=self.index,
            pipeline=self.pipeline
        )


    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.indices.exists(index=self.index)
        assert self.elasticsearch.ingest.get_pipeline(id=self.pipeline)