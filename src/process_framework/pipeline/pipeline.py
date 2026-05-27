# """"""""""""""""""""""""""""""""""""""""""""""""""""" #
#   This is the base class for Pipelines                #
#                                                       #
#   A pipeline consists of a series of `Steps`          #
#                                                       #
#   Pipelines are configured using Settings             #
#       passed in from their environment in a .env      #
#                                                       #
#   Pipelines can take CLI args                         #
#       if their settings are configured to read them   #
#                                                       #
# """"""""""""""""""""""""""""""""""""""""""""""""""""" #

# stdlib
from abc import abstractmethod, ABC
from typing import Self
from dataclasses import dataclass

# first-party (process_framework / process)
from ..steps import Step
from .clients import ClientsBase
from .references import ReferenceGraphBase
from .settings import SettingsBase
from ..exceptions import EarlyEscape
from ..composition.core import HasLogger

@dataclass
class PipelineBuilder[
    TSettings:SettingsBase,
    TReferences:ReferenceGraphBase,
    TClients:ClientsBase
    ](ABC):
    """Construct a pipeline from settings, references, and clients."""
    settings:TSettings
    references:TReferences
    clients:TClients

    @abstractmethod
    @classmethod
    def from_environment(cls, 
                         t_settings:type[TSettings], 
                         t_references:type[TReferences], 
                         t_clients:type[TClients], 
                         argsv=None) -> Self:
        """Initialize a builder from environment and CLI state."""
        settings = t_settings.from_environment(argsv)
        clients = t_clients.initialize(settings=settings)
        references = t_references.initialize()

        return cls(
            settings=settings,
            references=references,
            clients=clients
        )
    

    @abstractmethod
    def build_steps(self) -> list[Step]:
        """Construct the ordered steps for this pipeline."""
        ...


    def build_pipeline(self) -> 'Pipeline':
        """Build an executable pipeline instance."""
        steps = self.build_steps()
        return Pipeline(steps=steps)
    

    @classmethod
    def build_pipeline_from_environment(cls,
                                        t_settings:type[TSettings], 
                                        t_references:type[TReferences], 
                                        t_clients:type[TClients], 
                                        argsv=None) -> 'Pipeline':
        """Initialize a pipeline from environment and CLI state."""
        builder = cls.from_environment(t_settings, t_references, t_clients, argsv)
        return builder.build_pipeline()
        
        
@dataclass
class Pipeline(HasLogger):
    """Execute an ordered collection of steps."""
    steps:list[Step]
         
        
    def preflight(self) -> None:
        """Run preflight checks for all steps."""
        for step in self.steps:
            step.preflight()


    def do(self) -> None:
        """Execute pipeline steps in sequence."""
        self._info(f"Pipeline started")
        for step in self.steps:
            self._info(type(step).__name__)
            try:
                step.do()
            except EarlyEscape as e:
                self._info(
                    f"Pipeline terminated early on `Step` "
                    f"{type(step).__name__} with `EarlyEscape` exception {e}"
                )
                return
            
        self._info(f"Pipeline completed")


    def log_steps(self):
        for i, step in enumerate(self.steps):
            self._info(f'{i}\t{type(step).__name__}')


############# Example implementation ###################################################################

# class ExampleSettings(SettingsBase):
#     ...


# class ExampleReferences(ReferenceGraphBase):
#     ...


# class ExampleClients(ClientsBase):
#     ...


# class ExampleBuilder(PipelineBuilder[
#         ExampleSettings, 
#         ExampleReferences, 
#         ExampleClients
#     ]):
#     def build_steps(self) -> list[Step]:
#         return []
    

# ExampleBuilder.from_environment(ExampleSettings, ExampleReferences, ExampleClients).build_pipeline()

########################################################################################################