from abc import ABC, abstractmethod
from dataclasses import dataclass
from ...lifecycle.preflight import Preflightable
from ...references.composition.core import IGettable, ISettable, ITyped
from ...references.reference import Reference
from ...composition.retries import RetryArgs

class IGenerateValue[T](ABC):
    """interface for producing a value."""
    @abstractmethod
    def generate_value(self) -> T|None:
        ...


class ITransformValue[TIn, TOut](ABC):
    """interface for transforming an input value into an output value."""
    @abstractmethod
    def transform_value(self, input_:TIn) -> TOut:
        ...


""" core mixins for Steps """
class StepMixin(Preflightable, ABC):
    """mixin that participates in preflight chains with default logging."""
    def preflight(self) -> None:
        self._warn('base mixin preflight')
        super().preflight()


@dataclass
class HasInput[T](StepMixin):
    """mixin providing an input value source."""
    input_:IGettable[T]


@dataclass
class HasOptionalOutput[T](StepMixin):
    output_ : ISettable[T]|None

    
@dataclass
class HasOutput[T](StepMixin):
    """mixin providing an output value sink."""
    output_ : ISettable[T]


@dataclass
class HasReference[T](StepMixin):
    """mixin providing a generic reference."""
    reference : Reference[T]