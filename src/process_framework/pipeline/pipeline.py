from abc import ABC, abstractmethod
from .settings import SettingsBase
from .metadata import RunMetadata
from typing import Callable
from pydantic import BaseModel, Field, ConfigDict
from ..steps import Step
from elasticsearch import Elasticsearch

class PipelineBuilderBase[T: PipelineBase](ABC):
    
    def __init__(self, logging_callback):
        self.logging_callback=logging_callback


    @abstractmethod
    def get_name(self) -> str:
        pass

    
    @abstractmethod
    def build_pipeline(self, settings:SettingsBase, metadata:RunMetadata) -> T:
        pass


class PipelineBase(BaseModel, ABC):
    name:str
    logging_callback:Callable = Field(repr=False)
    
    @abstractmethod
    def get_steps(self) -> list[Step]:
        pass
    

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
    model_config = ConfigDict(arbitrary_types_allowed=True)
    elasticsearch:Elasticsearch = Field(repr=False)