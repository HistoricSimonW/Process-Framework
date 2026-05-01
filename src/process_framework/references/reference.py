from dataclasses import dataclass
from .interfaces.core import IGettable, ISettable, ITyped
from ..composition.mixins.logging import HasLogger

import reprlib

# one small, shared repr truncator
_repr = reprlib.Repr()
_repr.maxstring = 30
_repr.maxother = 30
_repr.maxlist = 3
_repr.maxtuple = 3
_repr.maxdict = 3

@dataclass(slots=True)
class Reference[T](IGettable[T], ISettable[T], ITyped[T], HasLogger):
    """a boxed reference to a typed value."""
    _type:type[T]
    value:T|None=None

    def get_type(self) -> type:
        return self._type

    def _on_set(self, value: T | None) -> None:
        self._info(f'{type(self.value)} -> {type(value)}')
        self.value = value

    def get_value(self) -> T:
        if self.value is None:
            raise ValueError()
        return self.value

    def has_value(self) -> bool:
        return self.value is not None
