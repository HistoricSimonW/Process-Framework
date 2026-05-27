from pydantic import BaseModel
from tenacity import RetryCallState, retry
from tenacity.stop import StopBaseT, stop_after_attempt
from tenacity.wait import WaitBaseT, wait_fixed, wait_exponential, wait_exponential_jitter
from typing import Any, Callable, TypeVar, ParamSpec
import logging

P = ParamSpec("P")
R = TypeVar("R")

class RetryArgs(BaseModel):
    """configuration for constructing a tenacity retry policy."""
    attempts:int=0
    wait_seconds:int|None=None
    wait_exponent:int|None=None
    wait_jitter:float|None=None

    log_error_level:int|None=None
    log_before_level:int|None=None

    def get_wait(self) -> WaitBaseT|None:
        """build the tenacity wait strategy from settings."""
        if self.wait_seconds is None:
            return None
        if self.wait_exponent is None:
            return wait_fixed(self.wait_seconds)
        if self.wait_jitter is None:
            return wait_exponential(multiplier=self.wait_seconds, exp_base=self.wait_exponent)
        return wait_exponential_jitter(initial=self.wait_seconds, exp_base=self.wait_exponent, jitter=self.wait_jitter)


    def get_stop(self) -> StopBaseT|None:
        """build the tenacity stop strategy from settings."""
        if self.attempts is None:
            return None
        return stop_after_attempt(self.attempts)


    def get_before(self):
        """build a before-attempt logging callback."""
        before_level = self.log_before_level
        if before_level is None:
            return None

        def before(state: RetryCallState) -> None:
            logging.log(
                before_level,
                "retry attempt %s for %s",
                state.attempt_number,
                getattr(state.fn, "__name__", state.fn),
            )

        return before

    def get_retry_error_callback(self):
        """build an exhausted-retry logging callback."""
        error_level = self.log_error_level
        if error_level is None:
            return None

        def on_error(state: RetryCallState) -> Any:
            exc = state.outcome.exception() if state.outcome else None
            logging.log(
                error_level,
                "retries exhausted after %s attempts: %s",
                state.attempt_number,
                exc,
            )
            return None

        return on_error


    def _get_retry_kwargs(self) -> dict:
        """assemble tenacity keyword arguments from settings."""
        kwargs = dict(
            stop = self.get_stop(),
            wait = self.get_wait(),
            before=self.get_before(),
            retry_error_callback=self.get_retry_error_callback()
        )
        return {k:v for k, v in kwargs.items() if v is not None}


    def get_retry(self) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """construct a tenacity retry decorator."""
        kwargs = self._get_retry_kwargs()
        return retry(**kwargs)
    

    def wrap(self, func:Callable[P, R]) -> Callable[P, R]:
        """wrap a callable with the configured retry policy."""
        return self.get_retry()(func)