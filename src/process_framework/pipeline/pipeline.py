from abc import ABC, abstractmethod
from .settings import SettingsBase
from .metadata import RunMetadata
from typing import Callable
from dataclasses import dataclass, field, fields, KW_ONLY
from ..steps import Step
from elasticsearch import Elasticsearch


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
    _=KW_ONLY,
    
    name:str
    logging_callback:Callable = field(repr=False, default=print)
    

    @abstractmethod
    def get_steps(self) -> list[Step]:
        pass
    

    def __post_init__(self) -> None:
        self.init_references()
        self._validate_fields()


    @abstractmethod
    def init_references(self):
        pass


    def _validate_fields(self) -> None:
        missing = [f for f in fields(self) if not f.init and f.name not in self.__dict__]
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



class ElasticSearchPipelineBase(PipelineBase):
    elasticsearch:Elasticsearch = field(repr=False)