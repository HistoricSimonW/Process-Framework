from abc import ABC, abstractmethod
from .settings import SettingsBase
from .metadata import RunMetadata
from typing import Callable

class PipelineBuilderBase(ABC):
    
    def __init__(self, logging_callback):
        self.logging_callback=logging_callback


    @abstractmethod
    def get_name(self) -> str:
        pass


    @abstractmethod
    def build_process(self, settings:SettingsBase, metadata:RunMetadata) -> Callable:
        """ build a pipeline process of References and Steps using normal control flow """
        pass


    def build_pipeline(self, settings:SettingsBase, metadata:RunMetadata):
        return Pipeline(self.build_process(settings, metadata))


class Pipeline:

    def __init__(self, process:Callable):
        self.process = process

    def execute(self):
        return self.process()