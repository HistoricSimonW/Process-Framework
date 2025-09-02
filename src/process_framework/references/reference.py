from dataclasses import dataclass, field
from typing import Type, Callable, Any, TypeVar
from collections.abc import Sized
import reprlib

# one small, shared repr truncator
_repr = reprlib.Repr()
_repr.maxstring = 30
_repr.maxother = 30
_repr.maxlist = 3
_repr.maxtuple = 3
_repr.maxdict = 3

T = TypeVar("T")

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
        assert self.value is not None, 'Reference has a None value'
        return self.value


    def _get_size(self) -> str|None:
        try:
            value = self.get_value()
        except:
            return None
        
        if shape:= getattr(value, 'shape', None):
            return shape
        elif not isinstance(value, str) and isinstance(value, Sized):
            return str(len(value))

        return None


    def _get_sample(self) -> str:
        try:
            value = self.get_value()
        except:
            return 'None'

        try:
            if hasattr(value, 'to_list'):
                return _repr.repr(value.to_list()) # type: ignore
            elif hasattr(value, 'to_dict'):
                return _repr.repr(value.to_dict('index')) # type: ignore
            
            return _repr.repr(value)
        
        except Exception:
            return _repr.repr(value)


    
    def __repr__(self):

        _size = self._get_size()
        _sample = self._get_sample()
                
        s = ', '.join(str(t) for t in (_size, _sample) if t)

        return f"Reference[{self._type.__name__}]({s})"