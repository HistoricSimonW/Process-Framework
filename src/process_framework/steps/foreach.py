from .step import Step
from ..references import Reference
from .step import Step
from ..references import Reference
from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
from typing import Type, Iterable, Callable, Any
from ..pipeline import SettingsBase
from ..pipeline import RunMetadata

"""
I want to be able to process batches of items in subprocesses
I don't want state to persist between subprocesses
if each subprocess is a pydantic model, with References to values
  and a method to get `steps`
I can use a SubProcessBuilder class to take an input from a ForEach
  and construct a SubProcess, with an initial `input_` value `Reference`
  and it can initialize its own steps
  this pattern allow me to inject references to ElasticSearch and so-on
    because the subprocessbuilder is constructed when its parent step is,
    and its parent step is constructed when the enclosing pipeline is defined
"""

class SubProcess[T](BaseModel, ABC):
    """ define References at the class level as Pydantic fields """
    input_:Reference[T]
    

    @abstractmethod
    def get_steps(self) -> list[Step]:
        pass
    

    def execute(self):
        print(f'initialized subprocess with input {self.input_}')
        for step in self.get_steps():
            print(type(step))
            step.do()
            print(self)


class SubProcessBuilder[T](ABC):
    """ responsible for spawning subprocesses, including injecting dependencies like ElasticSearch """
    def __init__(self, settings: SettingsBase, metadata: RunMetadata) -> None:
        self.settings = settings
        self.metadata = metadata
        
    @abstractmethod
    def build_subprocess(self, input_:T) -> SubProcess[T]:
        pass


class ForEach[T](Step):
    """ a step that applies a `SubProcess` to an iterable of values 
        if `item` implements an `__iter__` that yields `T`s, the default `item_iterer` will suffice,
        otherwise, pass in a callable that will get an iterable of `T`s for the input type (e.g., `dict.keys`)"""
    subprocess_builder:SubProcessBuilder[T]

    def __init__(self, items:Reference, subprocess_builder:SubProcessBuilder[T], item_iterer:Callable[[Any], Iterable[T]] = iter):
        self.items = items
        self.subprocess_builder = subprocess_builder
        self.item_iterer = item_iterer
    

    def do(self):
        values = self.items.get_value(True)

        for value in self.item_iterer(values):
            print('item', value)
            subprocess = self.subprocess_builder.build_subprocess(input_=value)
            subprocess.execute()
