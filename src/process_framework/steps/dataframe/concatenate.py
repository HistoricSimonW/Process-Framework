from process_framework.references.reference import Reference
from process_framework.steps import AssigningStep
from pandas import Series, DataFrame, concat
from typing import cast

class Concatenate[T:(Series, DataFrame)](AssigningStep[T]):
    """ concatenate a sequence of `concatenate_refs` `Reference[T]` using `pandas.concat` 
        where `T` is `Series` or `DataFrame`"""
    def __init__(self, assign_to: Reference, *concatenate_refs:Reference[T], overwrite:bool=True):
        super().__init__(assign_to, overwrite=overwrite)
        self.concatenate_refs = concatenate_refs

    
    def generate(self) -> T:
        values = [t.get_value() for t in self.concatenate_refs]
        return cast(T, concat(values))
        