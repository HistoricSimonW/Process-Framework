from dataclasses import dataclass, field
from typing import Type, Callable, Any, Sequence

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
        """ set the `value` of this reference to `value`; throw if `value` is not None and not an instance of `_type` """
        if value is not None and not isinstance(value, self._type):
            raise TypeError(f"Expected value of type {self._type}, got {type(value)}")
        
        if self.on_set:
            self.on_set(self, value)

        self.value = value

    def is_instance_of(self, class_or_tuple) -> bool:
        """ returns True if `value` is an instance of `class_or_tuple`; see `isinstance` """
        return isinstance(self.value, class_or_tuple)

    def has_value(self) -> bool:
        """ returns True if `value` is not None, else False """
        return self.value is not None

    def get_value(self) -> T:
        """ get `value` as a `T`; throw if `value` is None 
            useful for telling IDE type checkers that `value` is not None """
        assert self.value is not None, f'{self} has a None value'
        return self.value
    
    def __repr__(self):
        v = self.value
        vr = f'{v!r}'
        
        if not isinstance(v, str) and isinstance(v, Sequence):
            vr = len(v)

        return f"Reference[{self._type.__name__}]({vr})"