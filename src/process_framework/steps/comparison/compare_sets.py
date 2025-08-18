from ..assigning_step import AssigningStep
from ...references.reference import Reference
from typing import Iterable, Any
from pandas import Series

class CompareSets(AssigningStep[Series]):
    """ compare two Series of values and assign the set difference to a Reference[Series] """
    def __init__(self, assign_to:Reference[Series], a:Reference[Iterable], b:Reference[Iterable]):
        super().__init__(assign_to)
        self.a = a
        self.b = b

    def generate(self) -> Series:
        assert self.a.is_instance_of(Iterable)
        assert self.b.is_instance_of(Iterable)
        
        return Series(
            list(
                set(self.a.value).difference(self.b.value) # type: ignore
            )
        )