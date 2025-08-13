from dataclasses import dataclass, field
from typing import TypeVar, Generic, Type, Callable, Any

@dataclass
class Reference[T]:
    """ a nullable, 'boxed' reference to a [T] with runtime type checking """
    _type: Type[T] = field(repr=False)
    value: T | None = None

    on_set:Callable[['Reference[T]', T|None], Any]|None=None    # an optional callback, invoked on `set`

    def __post_init__(self):
        if self.value is not None and not isinstance(self.value, self._type):
            raise TypeError(f"Expected value of type {self._type}, got {type(self.value)}")
        
        assert self._type is not None

    def set(self, value: T | None):
        if value is not None and not isinstance(value, self._type):
            raise TypeError(f"Expected value of type {self._type}, got {type(value)}")
        
        if self.on_set:
            self.on_set(self, value)

        self.value = value

    def is_instance_of(self, cls) -> bool:
        return isinstance(self.value, cls)

    def has_value(self) -> bool:
        return self.value is not None

    def get_value(self, throw_on_none:bool=False) -> T | None:
        if throw_on_none and self.value is None:
            raise ValueError(f'{self} has a None value')
        return self.value
    
    def __repr__(self):
        return f"Reference[{self._type.__name__}]({self.value!r})"