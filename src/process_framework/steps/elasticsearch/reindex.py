from process_framework import Step, IGettable, ISettable
from process_framework.steps.composition.elasticsearch import HasElasticsearch, HasElasticsearchSourceIndex, HasElasticsearchTargetIndex
from pydantic import BaseModel, ConfigDict
from process_framework.steps.composition.elasticsearch.query import Query
from abc import ABC, abstractmethod
from typing import Any
from dataclasses import dataclass


class ReindexArgBase(BaseModel, ABC):
    """Base model for Elasticsearch reindex source and destination arguments."""
    index:str|IGettable[str]
    
    def get_index(self) -> str:
        """Return the concrete index name."""
        return self.index if isinstance(self.index, str) else self.index.get_value()

    @abstractmethod
    def get_args(self) -> dict[str, Any]:
        ...
        
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    
class ReindexSource(ReindexArgBase):
    """Source configuration for a reindex request."""
    query:Query|dict[str, Any]|None
    size:int|None=None
    sort:str|None=None
    
    def get_args(self) -> dict[str, Any]:
        args = self.model_dump(exclude_none=True, exclude={'index'})
        args['index'] = self.get_index()
        if isinstance(self.query, Query):
            args['query'] = self.query.get_query()
        return args
    

class ReindexDest(ReindexArgBase):
    """Destination configuration for a reindex request."""
    op_type:str|None=None
    pipeline:str|None=None
    routing:str|None=None
    version_type:str|None=None
    
    def get_args(self) -> dict[str, Any]:
        args = self.model_dump(exclude_none=True, exclude={'index'})
        args['index'] = self.get_index()
        return args
    

@dataclass(kw_only=True)
class Reindex(HasElasticsearch, Step):
    """Execute an Elasticsearch reindex operation."""
    source:ReindexSource
    dest:ReindexDest
    conflicts:str|None='proceed'
    max_docs:int|None=None
    refresh:bool=False
    slices:int|str|None=None
    requests_per_second:int|None=None
    script:dict|None=None
    timeout:str|None=None
    wait_for_completion:bool=True
    task_id:ISettable[str]|None=None
    
    def do(self) -> None:
        """Run the reindex request and optionally capture the task id."""
        self._info(
            f"reindex {self.source.get_index()} -> {self.dest.get_index()}"
        )
        
        response = self.elasticsearch.reindex(
            source=self.source.get_args(), 
            dest=self.dest.get_args(), 
            conflicts=self.conflicts, 
            max_docs=self.max_docs,
            refresh=self.refresh,
            requests_per_second=self.requests_per_second,
            script=self.script,
            slices=self.slices,
            timeout=self.timeout,
            wait_for_completion=self.wait_for_completion
        )
        
        if self.wait_for_completion:
            # we've already waited
            return
        
        # we know we're not in the wait_for_completion branch
        task_id = response.body['task']

        if self.task_id is not None:
            self.task_id.set_value(task_id)
        else:
            self._warn(f"`wait_for_completion = False` but `task_id` output is not set; task is still running: {task_id}")