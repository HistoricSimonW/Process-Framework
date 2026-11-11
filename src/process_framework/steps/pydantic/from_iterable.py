from collections.abc import Iterable
from dataclasses import dataclass

from pydantic import BaseModel

from process_framework import TransformingStep


@dataclass(kw_only=True)
class GenModelsFromIterable[TIn, TOut: BaseModel](TransformingStep[Iterable[TIn], Iterable[TOut]]):
    """Lazily validate iterable items as Pydantic models."""

    model_type: type[TOut]

    def transform_value(self, input_: Iterable[TIn]) -> Iterable[TOut]:
        """Generate a validated model for each input item."""
        return (
            self.model_type.model_validate(item)
            for item in input_
        )