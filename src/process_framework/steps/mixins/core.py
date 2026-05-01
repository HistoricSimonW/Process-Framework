""" core mixins for Steps """
from abc import ABC
from dataclasses import dataclass
from ...lifecycle.preflight import Preflightable
from ...references.interfaces.core import IGettable, ISettable, ITyped
from ...references.reference import Reference

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
class HasOutput[T](StepMixin):
    """mixin providing an output value sink."""
    output_ : ISettable[T]


@dataclass
class HasReference[T](StepMixin):
    """mixin providing a generic reference."""
    reference : Reference[T]