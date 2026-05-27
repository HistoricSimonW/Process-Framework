from ..step import AssigningStep
from ..composition.core import StepMixin
from dataclasses import field
from pathlib import Path
from abc import ABC, abstractmethod
import json
from pandas import DataFrame
import pandas as pd
from typing import Any, Callable
from io import TextIOWrapper

class HasPath(StepMixin):
    path:Path


class LoadFile[T](HasPath, AssigningStep[T]):

    @abstractmethod
    def generate_value(self) -> T | None:
        ...

    def preflight(self) -> None:
        assert self.path.exists()
        assert self.path.is_file()


class LoadJson[T:(list, dict)](LoadFile[T]):
    loader:Callable[[TextIOWrapper], T] = lambda path: json.load(path)

    def generate_value(self) -> T | None:
        with self.path.open('r') as f:
            return self.loader(f)
        
        
    def preflight(self) -> None:
        super().preflight()
        assert 'json' in self.path.name
        

class LoadCsvToDataFrame(LoadFile[DataFrame]):
    sep : str = ','
    header : Any
    read_csv_kwargs : dict = field(default_factory=dict)

    def generate_value(self) -> DataFrame | None:
        return pd.read_csv(
            self.path,
            sep=self.sep,
            header=self.header
            **self.read_csv_kwargs
        )
    

    def preflight(self) -> None:
        super().preflight()
        assert 'csv' in self.path.name


from ..step import TransformingStep
from typing import Iterable
from ...references.composition.core import IGettable
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