from process_framework import Step, IGettable, ISettable
from elasticsearch import NotFoundError
from ..composition.elasticsearch.query import Query, MatchAll, ValuesQuery
from ..composition.elasticsearch import HasElasticsearch
from time import sleep
from dataclasses import dataclass, field
from typing import Any
from collections.abc import Sequence
from itertools import batched

@dataclass(kw_only=True)
class AwaitTask(HasElasticsearch, Step):
    task_id:IGettable[str]
    await_task_interval:float=15
    await_task_timeout:int|None=None
    
    def do(self):
        task = self.task_id.get_value()
        
        # for 0 .. await_task_timeout, try to get the task
        #   if the task exists, the task is running
        #   if the task is not found, it's finished
        elapsed = 0
        interval = self.await_task_interval

        while True:
            try:
                r = self.elasticsearch.tasks.get(task_id=task)
                self._debug(str(r))
                if r.body.get('completed'):
                    self._info(str(r.body))
                    return
                
            except NotFoundError:
                self._info(f"Task not found; assuming complete: {task}")
                return
            
            sleep(interval)
            elapsed += interval
                