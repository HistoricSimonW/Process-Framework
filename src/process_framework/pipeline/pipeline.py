from abc import ABC, abstractmethod
from .settings import SettingsBase
from .metadata import RunMetadata
from typing import Callable
from dataclasses import dataclass, field, fields
from ..steps import Step


REQUIRED_POST_INIT = 'required_after_init'


def post_init_field():
    """ a non-init field that will raise an error in __post_init__ if it hasn't been set """
    return field(init=False, metadata={REQUIRED_POST_INIT:True})


def __dispose__(msg) -> None:
    """ a default logging_callback that consumes a message but does nothing with it """
    ...


class PipelineBuilderBase[T: PipelineBase](ABC):
    
    def __init__(self, logging_callback):
        self.logging_callback=logging_callback


    @abstractmethod
    def get_name(self) -> str:
        pass

    
    @abstractmethod
    def build_pipeline(self, settings:SettingsBase, metadata:RunMetadata) -> T:
        pass


@dataclass
class PipelineBase(ABC):
    logging_callback:Callable
    name:str
    
    @abstractmethod
    def get_steps(self) -> list[Step]:
        pass
    

    def __post_init__(self) -> None:
        # assign a guardian value to logging_callback so we can safely invoke it if it hasn't been set
        if not self.logging_callback:
            self.logging_callback = __dispose__
        
        # initialize references (in the instance `self` context)
        self.init_references()
        
        # ensure all required fields have been set
        self._validate_fields()


    @abstractmethod
    def init_references(self):
        pass


    def _validate_fields(self) -> None:
        missing = [f for f in fields(self) if not f.init and f.name not in self.__dict__ and REQUIRED_POST_INIT in f.metadata]
        if missing:
            raise ValueError(f"Uninitialized required fields: {', '.join(f.name for f in missing)}")
        

    def execute(self):
        self.logging_callback(self.name, 'started')
        self.logging_callback(self)

        for step in self.get_steps():
            self.logging_callback(type(step).__name__)
            step.do()
            self.logging_callback(self)
            
        self.logging_callback
        self.logging_callback(self.name, 'completed')