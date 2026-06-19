from process_framework import Step, IGettable
from elasticsearch import NotFoundError
from elastic_transport import ObjectApiResponse
from ..composition.elasticsearch import HasElasticsearch
from time import sleep
from dataclasses import dataclass
import tenacity
from elasticsearch import ConnectionError, ConnectionTimeout
from process_framework.context_managers.logging import suppress_logging
import logging
from datetime import timedelta
from typing import Any

@dataclass(kw_only=True)
class AwaitTask(HasElasticsearch, Step):
    """Wait for an Elasticsearch task to complete."""
    task_id:IGettable[str]
    interval:float=15
    timeout:int|None=None
    
    def has_timed_out(self, elapsed:float) -> bool:
        """Return whether the configured timeout has been exceeded."""
        return self.timeout is not None and elapsed >= self.timeout
    
    
    def format_status(self, response: ObjectApiResponse) -> str:
        """Return a compact task progress message."""
        status: dict[str, Any] = response.body.get("task", {}).get("status", {})

        values = {
            "batches": status.get("batches"),
            "created": status.get("created"),
            "updated": status.get("updated"),
            "total": status.get("total"),
            "deleted": status.get("deleted"),
        }

        return " ".join(f"{key}={value}" for key, value in values.items())

    
    @tenacity.retry(
            retry=tenacity.retry_if_exception_type(
            (ConnectionError, ConnectionTimeout, TimeoutError)
        ),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    def get_task(self) -> ObjectApiResponse:
        """Retrieve task status, retrying transient transport failures."""
        return self.elasticsearch.tasks.get(task_id=self.task_id.get_value())
     
     
    def do(self) -> None:
        """Poll the task API until completion, disappearance, or timeout."""
        task_id = self.task_id.get_value()
        
        elapsed = 0
        interval = self.interval

        with suppress_logging(
            ["elastic_transport.transport", "urllib3.connectionpool",],
            level=logging.WARNING,
        ):
            while True:
                try:
                    response = self.get_task()
                    ts = timedelta(seconds=elapsed)
                    status = self.format_status(response)
                    
                    self._debug(f'{ts}: {status}')

                    if response.body.get('completed'):
                        self._info(f'{ts}: {status}')
                        return
                    
                except NotFoundError:
                    self._info(f"Task not found; assuming complete: {task_id}")
                    return
                
                sleep(interval)
                elapsed += interval
                
                if self.has_timed_out(elapsed):
                    raise TimeoutError(f"Timed out waiting for Elasticsearch task: {task_id}")