from process_framework import Step, IGettable, ISettable
from elasticsearch import NotFoundError
from ..composition.elasticsearch.query import Query, MatchAll, ValuesQuery
from ..composition.elasticsearch import HasElasticsearch
from time import sleep
from dataclasses import dataclass, field
from typing import Any
from collections.abc import Sequence
from itertools import batched

MAX_ALLOWABLE_TERMS = 65536

@dataclass(kw_only=True)
class UpdateByQuery(HasElasticsearch, Step):
    """ perform an update-by-query operation on an index\n
     https://elasticsearch-py.readthedocs.io/en/v8.2.2/api.html?highlight=execute#elasticsearch.Elasticsearch.update_by_query """
    index:str|Sequence[str]|IGettable[str]|IGettable[Sequence[str]]
    pipeline:str
    query:dict|Query=field(default_factory=MatchAll)
    wait_for_completion:bool=True
    task_id:ISettable[str]|None=None # optional output for an awaitable task
    update_by_query_kwargs:dict[str, Any]=field(default_factory=dict)


    def get_query(self) -> dict:
        query = self.query
        
        if query is None:
            return MatchAll().get_query()
        
        if isinstance(query, dict):
            return query
        
        if isinstance(query, ValuesQuery) and len(query.get_values()) >= MAX_ALLOWABLE_TERMS:
            self._warn(f'length of query values > {MAX_ALLOWABLE_TERMS}, returning match_all')
            return MatchAll().get_query()
        
        return query.get_query()
    
    
    def get_index(self) -> str|Sequence:
        index = self.index
        if isinstance(index, IGettable):
            index = index.get_value()
        return index
    

    def do(self):

        index = self.get_index()

        response = self.elasticsearch.update_by_query(
            index=index,
            pipeline=self.pipeline,
            wait_for_completion=self.wait_for_completion,
            query=self.get_query(),
            **self.update_by_query_kwargs
        )

        self._info(f'performing update-by-query {self.pipeline}:{index}:({response})')

        if self.wait_for_completion:
            # we've already waited
            return
        
        # we know we're not in the wait_for_completion branch
        task_id = response.body['task']

        if self.task_id is not None:
            self.task_id.set_value(task_id)
        else:
            self._warn(f"`wait_for_completion = False` but `task_id` output is not set; task is still running: {task_id}")
        
        return

        
    def preflight(self):
                
        if isinstance(self.index, (str, Sequence)):
            assert self.elasticsearch.indices.exists(index=self.index)
        elif isinstance(self.index, IGettable):
            self._warn(f'`index` is a reference set at runtime')

        assert self.pipeline in self.elasticsearch.ingest.get_pipeline(id=self.pipeline)