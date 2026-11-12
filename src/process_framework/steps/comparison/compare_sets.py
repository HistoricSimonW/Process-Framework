from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import cast, Literal

from pandas import Series

from process_framework import AssigningStep, ValueOrReference, resolve


def as_series(values:set) -> Series:
    return Series(list(values))


@dataclass(kw_only=True)
class CompareSets[TIn, TOut=set[TIn]](AssigningStep[TOut]):
    """Compute the difference between two collections and transform the result."""
    a: ValueOrReference[Iterable[TIn]]
    b: ValueOrReference[Iterable[TIn]]
    result_func: Callable[[set[TIn]], TOut]= cast(
        Callable[[set[TIn]], TOut],
        lambda values: values,
    )
    method:Literal['difference', 'symmetric_difference']
    
    def generate_value(self) -> TOut:
        """Apply the selected set operation and transform its result."""
        a = set(resolve(self.a))
        b = set(resolve(self.b))

        if self.method == "difference":
            result = a.difference(b)
        elif self.method == "symmetric_difference":
            result = a.symmetric_difference(b)
        else:
            raise ValueError(f"Unsupported set comparison method: {self.method}")

        return self.result_func(result)