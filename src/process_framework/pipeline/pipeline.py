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
import logging
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

@dataclass
class PipelineBase[TSettings:SettingsBase, TReferences:ReferencesBase, TClients:ClientsBase](HasLogger, ABC):
    """base class for executable pipelines."""
    settings:TSettings
    clients:TClients
    references:TReferences
    steps:list[Step]
    
    @classmethod
    def from_environment(cls, argsv=None) -> Self:
        """build a pipeline from environment and cli settings."""
        cls._info('initializing pipeline')
        
        cls._info('... initializing settings')
        settings = cls.initialize_settings(argsv)
        
        cls._info('... initializing clients')
        clients = cls.initialize_clients(settings)
        clients.preflight()
        
        cls._info('... initializing references')
        refs = cls.initialize_references(settings)
        refs.preflight()

        cls._info('... initializing steps')
        steps = cls.initialize_steps(settings, refs, clients)

        pipeline = cls(settings, clients, refs, steps)
    
        cls._info("... performing preflight")
        pipeline.preflight()

        cls._info("initialization complete")
        return pipeline



    @classmethod
    def initialize_settings(cls, argsv=None) -> TSettings:
        """ extract a `Settings` model from an `argsv` list passed in (by a CLI) and the environment """
        settings_class = cls.get_settings_class()
        return settings_class.from_environment(argsv)
    
    
    @classmethod
    @abstractmethod
    def get_settings_class(cls) -> type[TSettings]:
        """ get this pipeline's Settings model """
        ...


    @classmethod
    @abstractmethod
    def initialize_clients(cls, settings: TSettings) -> TClients:
        """ initialize clients (elasticsearch, sqlalchemy engines, etc.)"""
        ...

    @classmethod
    @abstractmethod
    def initialize_references(cls, settings: TSettings) -> TReferences:
        """ initialize references (Reference[list], Reference[DataFrame], ColumnReference, etc.) """
        ...

    @classmethod
    @abstractmethod
    def initialize_steps(cls, settings: TSettings, refs: TReferences, clients: TClients) -> list[Step]:
        """ initialize steps """
        ...
       
        
    def preflight(self) -> None:
        """ make steps' preflight assertions """
        for step in self.steps:
            step.preflight()


    def do(self) -> None:
        """ execute the pipeline by iterating through its steps and doing them, detecting and handling any managed `EarlyEscape`s"""
        logging.info(f"Pipeline started")
        for step in self.steps:
            logging.info(type(step).__name__)
            try:
                step.do()
            except EarlyEscape as e:
                logging.info(f"Pipeline terminated early on `Step` {type(step).__name__} with `EarlyEscape` exception {e}")
                return
            
        logging.info(f"Pipeline completed")


    def log_steps(self):
        for i, step in enumerate(self.steps):
            logging.info(f'{i}\t{type(step).__name__}')