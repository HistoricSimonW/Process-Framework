# stdlib
import json
import logging
import os
import dotenv

from argparse import ArgumentParser, Namespace
from pathlib import Path
from types import SimpleNamespace
from typing import Tuple, Type

from abc import ABC, abstractmethod
from dataclasses import dataclass, fields

# third-party
from elasticsearch import Elasticsearch
from geopandas import GeoDataFrame
from pandas import DataFrame, Series
from sqlalchemy import URL, Engine, create_engine
from pydantic import BaseModel, ConfigDict

# first-party (process_framework / process)
from process_framework import Reference, Step


def load_json(path:Path) -> dict:
    return json.loads(path.read_text())


def sql_engine_from_config(path:Path) -> Engine:
    return create_engine(
        url=URL.create(**load_json(path))
    )


@dataclass
class ReferencesBase(ABC):
    
    def preflight(self) -> None:
        for field in fields(self):
            if getattr(self, field.name) is None:
                raise ValueError(f"Required reference {field} is not assigned")
            

@dataclass
class ClientsBase(ABC):
    
    def preflight(self) -> None:
        for field in fields(self):
            if getattr(self, field.name) is None:
                raise ValueError(f"Required reference {field} is not assigned")


class SettingsBase(BaseModel, ABC):

    model_config = ConfigDict(
        extra='ignore'
    )


class PipelineBase[TSettings:SettingsBase, TReferences:ReferencesBase, TClients:ClientsBase](ABC):
    
    def __init__(self, argsv=None) -> None:
        logging.info('initializing pipeline')
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


    def get_args_from_environment(self, argsv=None) -> dict:
        try:
            from dotenv import load_dotenv
            for fn in ('.env', '.env.local'):
                load_dotenv(Path(fn), override=True)
        except:
            ...
            
        # try:
        #     parser = ArgumentParser()
        #     args = parser.parse_args(argsv).__dict__
        # except:
        #     args = dict()    

        return dict() | os.environ #| args


    def initialize_settings(self, argsv=None) -> TSettings:
        args:dict = self.get_args_from_environment(argsv)
        return self.extract_settings(args)
    

    @abstractmethod
    def extract_settings(self, args:dict) -> TSettings:
        """ extract a `Settings` model from an `args` dict passet in from the environment """
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