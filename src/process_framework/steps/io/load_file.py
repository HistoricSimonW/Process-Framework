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

    def preflight(self) -> None:
        if not self.path.exists():
            raise Exception(f'Path {self.path} does not exist')
        return super().preflight()


class LoadFile[T](HasPath, AssigningStep[T]):

    @abstractmethod
    def generate_value(self) -> T | None:
        ...

    def preflight(self) -> None:
        if not self.path.is_file():
            raise Exception(f'Path {self.path} is not a file')
        return super().preflight()


class LoadJson[T:(list, dict)](LoadFile[T]):
    loader:Callable[[TextIOWrapper], T] = lambda path: json.load(path)

    def generate_value(self) -> T | None:
        with self.path.open('r') as f:
            return self.loader(f)
        
        
    def preflight(self) -> None:
        super().preflight()
        assert 'json' in self.path.suffix
        

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
