from pydantic import BaseModel
from typing import Annotated
from abc import ABC
from dataclasses import dataclass
from abc import ABC

@dataclass
class Masked:
    """Mask values for safe display in repr output."""
    chars:int=4

    def mask(self, value) -> str:
        """Return a partially masked string representation of value."""
        v = str(value)
        if len(v) <= self.chars*2:
            return "*" * len(v)

        return f"{v[:self.chars]}{'*' * (len(v) - self.chars * 2)}{v[-self.chars:]}"
    

class HasMaskedFields(BaseModel, ABC):
    """Mixin adding repr masking support via Annotated field metadata. 
       use as: Annotated[str, Masked(2)] """
    
    @classmethod
    def __get_mask__(cls, name:str|None) -> Masked|None:
        """Return the Masked metadata configured for a field."""
        if name is None:
            return None
        
        field = cls.model_fields.get(name)
        if field is None:
            return None
        
        return next((meta for meta in field.metadata if isinstance(meta, Masked)), None)
        

    def __repr_args__(self):
        """Yield repr arguments with masked values where configured."""
        for name, value in super().__repr_args__():
            masked = self.__get_mask__(name)
            if masked is None:
                yield name, value
            else:
                yield name, masked.mask(value)


if __name__ == '__main__':
    from typing import Annotated
    class Demo(HasMaskedFields, BaseModel):
        name:Annotated[str, Masked(2)]

    d = Demo(name='loooooooooooooooong')
    print(repr(d))