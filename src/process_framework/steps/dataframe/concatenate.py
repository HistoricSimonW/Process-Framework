from ...references.composition.core import IGettable
from ..composition.core import ITransformValue
from process_framework.steps import AssigningStep
from pandas import Series, DataFrame, concat
from typing import cast, Iterable
from dataclasses import dataclass

@dataclass
class Concatenate[T:(Series, DataFrame)](ITransformValue[Iterable[T], T], AssigningStep[T]):
    """ concatenate a sequence of `concatenate_refs` `Reference[T]` using `pandas.concat` 
        where `T` is `Series` or `DataFrame`"""
    concatenate_refs:list[IGettable[T]]

    def _gen_values(self) -> Iterable[T]:
        for ref in self.concatenate_refs:
            yield ref.get_value()
            
    def transform_value(self, input_: Iterable[T]) -> T:
        return cast(T, concat(input_))

    def generate_value(self) -> T | None:
        values = self._gen_values()
        return self.transform_value(values)
        