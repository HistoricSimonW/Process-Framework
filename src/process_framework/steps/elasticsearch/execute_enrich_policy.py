from ..step import Step
from elasticsearch.client import Elasticsearch
from elasticsearch import NotFoundError
from logging import info
from time import sleep

# https://elasticsearch-py.readthedocs.io/en/v8.2.2/api.html?highlight=execute#elasticsearch.client.EnrichClient.execute_policy:~:text=the%20enrich%20policy-,execute,_policy,-(*%2C%20name%3A%20str
class ExecutePolicy(Step):
    """ execute the specified enrich policy """
    def __init__(self, elasticsearch:Elasticsearch, policy:str, await_task:bool=True, await_task_interval:float=1, await_task_timeout:int=120) -> None:
        self.elasticsearch = elasticsearch
        self.policy = policy
        self.await_task = await_task
        self.await_task_interval = await_task_interval
        self.await_task_timeout = await_task_timeout


    def do(self):
        enrich = self.elasticsearch.enrich
        
        response = enrich.execute_policy(
            name=self.policy,
            wait_for_completion=False
        )

        info(f'executed policy {self.policy}:({response})')

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
                
        info(f'executed enrich policy {self.policy}')

    
    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.enrich.get_policy(name=self.policy)