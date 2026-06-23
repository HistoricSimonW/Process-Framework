from logging import info
from dataclasses import dataclass
from process_framework.steps.composition.elasticsearch.core import HasElasticsearch
from process_framework import Step, ISettable

# https://elasticsearch-py.readthedocs.io/en/v8.2.2/api.html?highlight=execute#elasticsearch.client.EnrichClient.execute_policy:~:text=the%20enrich%20policy-,execute,_policy,-(*%2C%20name%3A%20str

@dataclass(kw_only=True)
class ExecutePolicy(HasElasticsearch, Step):
    """ execute the specified enrich policy """
    policy:str
    wait_for_completion:bool=True
    task_id:ISettable[str]|None=None # optional output for an awaitable task
    
    def do(self):
        # get the enrich client
        enrich = self.elasticsearch.enrich
        
        response = enrich.execute_policy(
            name=self.policy,
            wait_for_completion=self.wait_for_completion
        )

        info(f'executed policy {self.policy}:({response})')

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
        assert self.elasticsearch.info()
        assert self.elasticsearch.enrich.get_policy(name=self.policy)