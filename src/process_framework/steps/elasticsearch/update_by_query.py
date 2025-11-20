from ..step import Step
from ...references import Reference
from ...references.dataframe import ColumnReference, IndexReference
from elasticsearch.client import Elasticsearch
from typing import Sequence
from elasticsearch import NotFoundError
from logging import info
from time import sleep
from pandas import Index, Series

# https://elasticsearch-py.readthedocs.io/en/v8.2.2/api.html?highlight=execute#elasticsearch.Elasticsearch.update_by_query
class UpdateByQuery(Step):
    """ perform an update-by-query operation on an index """
    # TODO : build option to pass and do a query
    def __init__(self, elasticsearch:Elasticsearch, index:str|Sequence[str], pipeline:str, 
                 query:dict|None=None, _ids:list|Reference[list]|Reference[Series]|Reference[Index]|None=None,
                 execute_without_query:bool=True, _ids_as_terms_field:str|None=None,   # if a query isn't provided, or if _ids aren't specified, run the pipeline over the whole index
                 *, await_task:bool=True, await_task_interval:float=1, await_task_timeout:int=120) -> None:
        self.elasticsearch = elasticsearch
        self.index = index
        self.pipeline = pipeline
        
        self.query=query
        self._ids = _ids
        self._ids_as_terms_field = _ids_as_terms_field
        self.execute_without_query = execute_without_query

        self.await_task = await_task
        self.await_task_interval = await_task_interval
        self.await_task_timeout = await_task_timeout


    def get_ids(self) -> list|None:
        """ handle _ids from list or Reference[list] or Reference[Series] or None to list or None """
        _ids = self._ids
        if _ids is None or isinstance(_ids, list):
            return _ids

        if isinstance(_ids, Reference) and _ids.is_instance_of(list):
            value = _ids.get_value()
            assert isinstance(value, list)# or value is None
            return value

        if (isinstance(_ids, Reference) and _ids.is_instance_of((Series, Index))) or isinstance(_ids, (ColumnReference, IndexReference)):
            value = _ids.get_value()
            if isinstance(value, (Series, Index)):
                return value.to_list()
            return None

        raise Exception()


    # TODO: Wrap this in a `ReferableQuery` class that constructs queries around `Reference`s
    def get_query(self) -> dict|None:
        if isinstance(self.query, dict):
            return self.query
        
        if self._ids is None or (isinstance(self._ids, Reference) and not self._ids.has_value()):
            return None
        
        _ids = self.get_ids()

        return {'terms':{self._ids_as_terms_field:_ids}} if self._ids_as_terms_field else {'ids':{'values':_ids}}
        
        
    
    def do(self):
        query = self.get_query()

        if query is None and not self.execute_without_query:
            info('`get_query` returned a `None` query; were no _ids provided? returning early')
            return 
        
        response = self.elasticsearch.update_by_query(
            index=self.index,
            pipeline=self.pipeline,
            wait_for_completion=False,
            query=query
        )

        info(f'performing update-by-query {self.pipeline}:{self.index}:({response})')

        if not self.await_task:
            return None
        
        task = response.body['task']

        # for 0 .. await_task_timeout, try to get the task
        #   if the task exists, the task is running
        #   if the task is not found, it's finished
        for _ in range(self.await_task_timeout):
            try:
                r = self.elasticsearch.tasks.get(task_id=task)
                if r.body.get('completed'):
                    info(f'{r.body}')
                    break
            except NotFoundError:
                break
            sleep(self.await_task_interval)
                
        info(f'performed update-by-query {self.pipeline}:{self.index}')

    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.indices.exists(index=self.index)
        assert self.elasticsearch.ingest.get_pipeline(id=self.pipeline)