# stdlib
import json
import logging
from pathlib import Path
from abc import abstractmethod, ABC

# third-party
from sqlalchemy import URL, Engine, create_engine

# first-party (process_framework / process)
from process_framework import Step
from process_framework.pipeline.clients import ClientsBase
from process_framework.pipeline.references import ReferencesBase
from process_framework.pipeline.settings import SettingsBase


def load_json(path:Path) -> dict:
    """ deserialize a json file at `path` to a dict """
    return json.loads(path.read_text())


def sql_engine_from_config(path:Path) -> Engine:
    """ construct a sqlalchemy Engine from a json file at `path` """
    return create_engine(
        url=URL.create(**load_json(path))
    )


class PipelineBase[TSettings:SettingsBase, TReferences:ReferencesBase, TClients:ClientsBase](ABC):
    """ base class for Pipelines """
    def __init__(self, argsv=None) -> None:
        logging.info('initializing pipeline')
        
        logging.info('  initializing settings')
        self.settings = self.initialize_settings(argsv)
        
        logging.info('  initializing references')
        self.refs = self.initialize_references(self.settings)
        self.refs.preflight()

        logging.info('  initializing clients')
        self.clients = self.initialize_clients(self.settings)
        self.clients.preflight()

        logging.info('  initializing steps')
        self.steps = self.initialize_steps(self.settings, self.refs, self.clients)
        
        logging.info('  performing preflight')
        self.preflight()
        
        logging.info('initialization complete')


    def initialize_settings(self, argsv=None) -> TSettings:
        """ extract a `Settings` model from an `args` dict passet in from the environment """
        settings_class = self.get_settings_class()
        return settings_class.from_environment(argsv)
    
    
    @abstractmethod
    def get_settings_class(self) -> type[TSettings]:
        """ get the type of the pipeline's Settings model """
        ...


    @abstractmethod
    def initialize_clients(self, settings: TSettings) -> TClients:
        """ initialize clients (elasticsearch, sqlalchemy engines, etc.)"""
        ...

    @abstractmethod
    def initialize_references(self, settings: TSettings) -> TReferences:
        """ initialize references (Reference[list], Reference[DataFrame], ColumnReference, etc.) """
        ...

    @abstractmethod
    def initialize_steps(self, settings: TSettings, refs: TReferences, clients: TClients) -> list[Step]:
        """ initialize steps """
        ...
       
        
    def preflight(self) -> None:
        """ make steps' preflight assertions """
        for step in self.steps:
            step.preflight()


    def do(self):
        for step in self.steps:
            logging.info(type(step).__name__)
            step.do()


    def log_steps(self):
        for i, step in enumerate(self.steps):
            logging.info(f'{i}\t{type(step).__name__}')