from .step import AssigningStep
from .composition.core import StepMixin
from dataclasses import field
from pathlib import Path
from abc import ABC, abstractmethod
import json
from pandas import DataFrame
import pandas as pd
from typing import Any, Callable
from io import TextIOWrapper

from .step import TransformingStep
from typing import Iterable
from ..references.composition.core import IGettable
from dataclasses import dataclass

# sentinel throw object
_THROW = object() 

@dataclass
class HasMapper[TIn, TOut](StepMixin):
    mapper:dict[TIn, TOut] | IGettable[dict[TIn, TOut]]
    default:TOut | object = _THROW

    def get_mapper(self) -> dict[TIn, TOut]:
        if isinstance(self.mapper, dict):
            return self.mapper
        if isinstance(self.mapper, IGettable):
            return self.mapper.get_value()
        raise ValueError()


@dataclass
class MapIter[TIn, TOut](HasMapper, TransformingStep[Iterable[TIn], Iterable[TOut]]):

    def transform_value(self, input_: Iterable[TIn]) -> Iterable[TOut]:
        mapper = self.get_mapper()

        getter: Callable[[TIn], TOut] = lambda g: mapper.get(g, self.default)

        if self.default is _THROW:
            getter = lambda g: mapper[g]

        for in_ in input_:
            yield getter(in_)


@dataclass
class ConcatIters[T](AssigningStep[Iterable[T]]):
    iterables:list[Iterable[T]|IGettable[Iterable[T]]]

    def generate_value(self) -> Iterable[T] | None:
        for iterable in self.iterables:
            if isinstance(iterable, IGettable):
                iterable = iterable.get_value()
            
            yield from iterable