from dataclasses import dataclass
from .composition.core import IGettable, ISettable, ITyped
from ..composition.core.logging import HasLogger


@dataclass(slots=True)
class Reference[T](IGettable[T], ISettable[T], ITyped[T], HasLogger):
    """a boxed reference to a typed value."""
    _type:type[T]
    value:T|None=None

    def get_type(self) -> type:
        return self._type

    def _on_set(self, value: T | None) -> None:
        self._debug(f'{type(self.value)} -> {type(value)}')
        self.value = value

    def get_value(self) -> T:
        if self.value is None:
            raise ValueError("`get_value` was called on an unset `Reference`")
        return self.value

    def has_value(self) -> bool:
        return self.value is not None
