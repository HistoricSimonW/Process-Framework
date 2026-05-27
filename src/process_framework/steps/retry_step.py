from .step import Step
from ..composition.retries import RetryArgs
from dataclasses import dataclass

@dataclass
class Retry(Step):
    """retry a step using the configured retry policy."""
    step:Step
    retry:RetryArgs

    def do(self) -> None:
        return self.retry.wrap(self.step.do)()
    
    def preflight(self) -> None:
        return self.step.preflight()