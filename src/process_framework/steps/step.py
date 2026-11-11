from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..lifecycle.preflight import Preflightable
from .composition.core import (
    HasInput,
    HasOutput,
    IGenerateValue,
    ITransformValue,
)
from ...process_framework import resolve


@dataclass(kw_only=True)
class Step(Preflightable, ABC):
    """Base step with preflight and execution lifecycle."""

    def preflight(self) -> None:
        self._info("base preflight")
        super().preflight()

    @abstractmethod
    def do(self) -> None:
        """Execute the step."""
        ...


@dataclass(kw_only=True)
class AssigningStep[T](Step, HasOutput[T], IGenerateValue[T]):
    """Assign a generated value to an output."""

    overwrite: bool = True
    """ if False, generation is skipped when output_ has a value """

    def do(self) -> None:
        """Generate and assign a value to an output."""
        if self.overwrite or not self.output_.has_value():
            self.output_.set_value(self.generate_value())


@dataclass(kw_only=True)
class TransformingStep[TIn, TOut](
        AssigningStep[TOut],
        HasInput[TIn],
        ITransformValue[TIn, TOut],
    ):
    """Assign the result of transforming an input."""

    def generate_value(self) -> TOut:
        """Generate a value by transforming the resolved input."""
        return self.transform_value(resolve(self.input_))

    @abstractmethod
    def transform_value(self, input_: TIn) -> TOut:
        """Transform an input value."""
        ...


@dataclass(kw_only=True)
class ModifyingStep[T](TransformingStep[T, T]):
    """Transform a value without changing its type."""