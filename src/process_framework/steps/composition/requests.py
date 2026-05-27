from .core import StepMixin
from requests import Session, Response
from dataclasses import dataclass, field
from pydantic import BaseModel
from typing import Iterable
from ...composition.retries import RetryArgs
from abc import ABC, abstractmethod

@dataclass
class HasHttpSession(StepMixin):
    """mixin providing a requests session."""
    session:Session = field(default_factory=Session)


@dataclass
class HasHttpGetter[T:BaseModel](HasHttpSession, ABC):
    """mixin for fetching and validating HTTP GET responses."""
    retry:RetryArgs|None=None

    @abstractmethod
    def build_request_kwargs(self) -> dict:
        """build request kwargs for session.get."""
        ...


    def get_response(self, request:dict) -> Response:
        """execute a GET request with optional retry."""
        call = lambda: self.session.get(**request)
        if self.retry is not None:
            call = self.retry.wrap(call)
        response = call()
        response.raise_for_status()
        return response
    

    @abstractmethod
    def validate_response(self, response:Response) -> T:
        """validate an HTTP response into a typed model."""
        ...


@dataclass
class HasHttpGetterGenerator[T:BaseModel](HasHttpGetter[T]):
    """mixin for generating and validating multiple GET responses."""
    @abstractmethod
    def gen_requests(self) -> Iterable[dict]:
        """yield request kwargs for session.get."""
        ...

    
    def gen_validated_responses(self) -> Iterable[T]:
        """yield validated responses for generated requests."""
        for req in self.gen_requests():
            response = self.get_response(req)
            yield self.validate_response(response)
