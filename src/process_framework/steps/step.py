from abc import ABC, abstractmethod
from dataclasses import dataclass, KW_ONLY
from ..lifecycle.preflight import Preflightable
from .composition.core import IGenerateValue, ITransformValue, HasOutput, HasInput

@dataclass
class Step(Preflightable, ABC):
    """abstract pipeline step with preflight and execution lifecycle."""
    def preflight(self) -> None:
        self._info('base preflight')
        super().preflight()

    @abstractmethod
    def do(self):
        ...


@dataclass
class AssigningStep[T](Step, HasOutput[T], IGenerateValue[T]):
    """step that assigns a generated value to its output."""
    _ : KW_ONLY
    overwrite:bool=True
    def do(self):
        if not self.output_.has_value() or self.overwrite:
            value = self.generate_value()
            self.output_.set_value(value)
    

@dataclass
class TransformingStep[TIn, TOut](AssigningStep[TOut], HasInput[TIn], ITransformValue[TIn, TOut]):
    """step that generates output by transforming its input."""
    def generate_value(self) -> TOut | None:
        return self.transform_value(self.input_.get_value())

    @abstractmethod
    def transform_value(self, input_: TIn) -> TOut:
        ...


@dataclass
class ModifyingStep[T](TransformingStep[T, T]):
    """specialised transforming step where input and output types are the same."""
    ...