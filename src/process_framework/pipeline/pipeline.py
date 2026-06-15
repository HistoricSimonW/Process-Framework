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
from .references import ReferencesBase
from .settings import SettingsBase
from ..exceptions import EarlyEscape
from ..composition.core import HasLogger

@dataclass(kw_only=True)
class PipelineDefinitionBase[
        TSettings:SettingsBase,
        TReferences:ReferencesBase,
        TClients:ClientsBase
    ](ABC):
    """Construct a pipeline from settings, references, and clients.\n
        \tTSettings\n
        \tTReferences\n
        \tTClients"""
    settings:TSettings
    references:TReferences
    clients:TClients
    
    @classmethod
    @abstractmethod
    def get_settings_type(cls) -> type[TSettings]:
        ...

    @classmethod
    @abstractmethod
    def initialize_references(cls) -> TReferences:
        ...

    @classmethod
    @abstractmethod
    def get_clients_type(cls) -> type[TClients]:
        ...

    @classmethod
    def from_environment(cls, argsv=None) -> Self:
        """Initialize a builder from environment and CLI state."""
        settings = cls.get_settings_type().from_environment(argsv)
        clients = cls.get_clients_type().initialize(settings=settings)
        references = cls.initialize_references()

        return cls(
            settings=settings,
            references=references,
            clients=clients
        )
    

    @abstractmethod
    def instantiate_steps(self) -> list[Step]:
        """Construct the ordered steps for this pipeline."""
        ...


    def instantiate_pipeline(self) -> 'Pipeline':
        """Build an executable pipeline instance."""
        steps = self.instantiate_steps()
        return Pipeline(steps=steps)
    

    @classmethod
    def instantiate_pipeline_from_environment(cls, argsv=None) -> 'Pipeline':
        """Initialize a pipeline from environment and CLI state."""
        definition = cls.from_environment(argsv)
        return definition.instantiate_pipeline()


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

    def log_steps(self) -> None:
        for i, step in enumerate(self.steps):
            self._info(f'{i}\t{type(step).__name__}')