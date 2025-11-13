from ..step import Step
from elasticsearch.client import Elasticsearch
from typing import Sequence
from elasticsearch import NotFoundError
from logging import info
from time import sleep

# https://elasticsearch-py.readthedocs.io/en/v8.2.2/api.html?highlight=execute#elasticsearch.Elasticsearch.update_by_query
class UpdateByQuery(Step):
    """ perform an update-by-query operation on an index """
    # TODO : build option to pass and do a query
    def __init__(self, elasticsearch:Elasticsearch, index:str|Sequence[str], pipeline:str, await_task:bool=True, await_task_interval:float=1, await_task_timeout:int=120) -> None:
        self.elasticsearch = elasticsearch
        self.index = index
        self.pipeline = pipeline
        self.await_task = await_task
        self.await_task_interval = await_task_interval
        self.await_task_timeout = await_task_timeout


    
    def do(self):
        response = self.elasticsearch.update_by_query(
            index=self.index,
            pipeline=self.pipeline,
            wait_for_completion=False
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
                self.elasticsearch.tasks.get(task_id=task)
            except NotFoundError:
                break
            sleep(self.await_task_interval)
                
        info(f'performed update-by-query {self.pipeline}:{self.index}')

    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.indices.exists(index=self.index)
        assert self.elasticsearch.ingest.get_pipeline(id=self.pipeline)