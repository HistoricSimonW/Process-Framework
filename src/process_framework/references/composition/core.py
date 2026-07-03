from abc import ABC, abstractmethod
from typing import Tuple, Literal, Any, TypeVar, TypeAlias


class IHasable(ABC):
    """interface for objects that can announce if they have a value."""
    @abstractmethod
    def has_value(self) -> bool:
        ...


class IGettable[T](IHasable, ABC):
    """interface for objects that can provide a value."""
    @abstractmethod
    def get_value(self) -> T:
        ...
    
    
    def try_get_value(self) -> Tuple[Literal[True], T] | Tuple[Literal[False], None]:
        if self.has_value():
            return (True, self.get_value())
        return (False, None)


class ITyped[T](ABC):
    """interface exposing a runtime type for values."""
    @abstractmethod
    def get_type(self) -> type[T]:
        ...

    def is_instance_of(self, obj:object) -> bool:
        return isinstance(obj, self.get_type())


class ISettable[T](IHasable, ITyped[T]):
    """interface for objects that can accept values with type checking."""
    def set_value(self, value:T|None) -> None:
        if not self.can_set(value):
            raise ValueError(
                f"Cannot set value of type {type(value).__name__}; "
                f"expected {self.get_type().__name__} or None."
            )
        self._on_set(value)

    @abstractmethod
    def _on_set(self, value:T|None) -> None:
        ...

    def can_set(self, value:T|Any|None) -> bool:
        return value is None or isinstance(value, self.get_type())


T = TypeVar("T")
ValueOrReference:TypeAlias = T | IGettable[T]

def resolve[T](ref_or_value:ValueOrReference[T]) -> T:
    """Return the concrete value from a value or an `IGettable` reference."""
    if isinstance(ref_or_value, IGettable):
        return ref_or_value.get_value()
    return ref_or_value