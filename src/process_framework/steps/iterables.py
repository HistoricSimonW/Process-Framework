from dataclasses import dataclass
from typing import Mapping, Final, Callable, Iterable

from ..references.composition.core import IGettable
from .composition.core import StepMixin
from .step import AssigningStep, TransformingStep

class _ThrowType:
    """ a sentinel class indicating "if this is the default, throw on a missing key" """
    pass


_THROW: Final = _ThrowType() # this should not be reassigned

@dataclass
class HasMapping[TIn, TOut](StepMixin):
    """Mixin for steps that map input values through a dictionary-like lookup."""
    mapping:Mapping[TIn, TOut] | IGettable[Mapping[TIn, TOut]]
    default:TOut | _ThrowType = _THROW

    def get_mapping(self) -> Mapping[TIn, TOut]:
        value = self.mapping
        
        if isinstance(value, IGettable):
            value = value.get_value()
        
        if not isinstance(value, Mapping):
            raise TypeError(f"Expected Mapping, got {type(value).__name__}")
        
        return value
    

    def get_lookup_strategy(self, mapping:Mapping[TIn, TOut]) -> Callable[[TIn], TOut]:
        """ get a lookup strategy; throwing on missing keys if `default` is `_ThrowType` else `.get`ing with a default """
        default = self.default

        if isinstance(default, _ThrowType):
            # if `default` is the _ThrowType sentinel, use __getitem__ which will throw on a missing key
            return mapping.__getitem__
        else:
            # else `get` the value from the mapping, returning `default` on a missing key
            return lambda key: mapping.get(key, default)



@dataclass
class MapIterable[TIn, TOut](HasMapping[TIn, TOut], TransformingStep[Iterable[TIn], Iterable[TOut]]):
    """Map each item in an iterable through a configured mapping."""

    def transform_value(self, input_: Iterable[TIn]) -> Iterable[TOut]:
        mapping = self.get_mapping()
        lookup:Callable[[TIn], TOut] = self.get_lookup_strategy(mapping)

        for value in input_:
            yield lookup(value)


@dataclass
class ChainIterables[T](AssigningStep[Iterable[T]]):
    """ chain the values in a list of iterables or references to iterables """
    iterables:list[Iterable[T]|IGettable[Iterable[T]]]

    def generate_value(self) -> Iterable[T] | None:
        for iterable in self.iterables:
            if isinstance(iterable, IGettable):
                iterable = iterable.get_value()
            
            yield from iterable