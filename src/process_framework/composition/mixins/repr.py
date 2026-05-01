from dataclasses import dataclass, field
from typing import Any, Sized, ClassVar
from abc import ABC, abstractmethod
import reprlib

# one small, shared repr truncator
_repr = reprlib.Repr(
    maxlist=3,
    maxstring = 30, 
    maxother = 30,
    maxtuple = 3, 
    maxdict = 3
)

@dataclass
class HasRepr(ABC):
    _repr:ClassVar[reprlib.Repr] = _repr

    @abstractmethod
    def _get_repr_value(self):
        ...
    
    @staticmethod
    def _get_size(x:Any) -> Any:
        if x is None:
            return None
        
        if shape := getattr(x, 'shape', None):
            return shape
        
        if not isinstance(x, str) and isinstance(x, Sized):
            return len(x)
        
        return None
    
    @staticmethod
    def _get_sample(x:Any) -> Any:       
        # try to handle Pandas DataFrame/Series-likes
        try:
            if hasattr(x, 'to_list'):
                return x.head(_repr.maxother).to_list() # type: ignore
            
            if hasattr(x, 'to_dict'):
                return x.head(_repr.maxother).to_dict('index') # type: ignore
        except:
            return x

        return x
    

    def _get_repr_type(self) -> type|None:
        return None
    
        
    def __repr__(self) -> str:
        x = self._get_repr_value()
        type_ = self._get_repr_type()
                
        parts = (
            self._get_size(x),
            self._get_sample(x),
        )

        inner = ", ".join(_repr.repr(t) for t in parts if t is not None)

        type_suffix = f'[{type_.__name__}]' if type_ else ''

        return f'{type(self).__name__}{type_suffix}({inner})'