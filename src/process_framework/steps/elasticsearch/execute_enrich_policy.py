from ..step import Step
from elasticsearch import NotFoundError
from logging import info
from time import sleep
from dataclasses import dataclass
from process_framework.steps.composition.elasticsearch.core import HasElasticsearch

# https://elasticsearch-py.readthedocs.io/en/v8.2.2/api.html?highlight=execute#elasticsearch.client.EnrichClient.execute_policy:~:text=the%20enrich%20policy-,execute,_policy,-(*%2C%20name%3A%20str

@dataclass(kw_only=True)
class ExecutePolicy(HasElasticsearch, Step):
    """ execute the specified enrich policy """
    policy:str
    await_task:bool=True
    await_task_interval:float=1
    await_task_timeout:int=120
    

    def do(self):
        # get the enrich client
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