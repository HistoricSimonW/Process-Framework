# """"""""""""""""""""""""""""""""""""""""""""""""""""" #
#   This is the base class for Scripts                  #
#                                                       #
#   A script is a lightweight, imperative alternative   #
#    to Pipeline                                        #
#                                                       #
#   Scripts are configured using Settings               #
#       passed in from their environment in a .env      #
#                                                       #
#   Scripts can take CLI args                           #
#       if their settings are configured to read them   #
#                                                       #
# """"""""""""""""""""""""""""""""""""""""""""""""""""" #

from process_framework.pipeline.clients import ClientsBase
from process_framework.pipeline.settings import SettingsBase

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Self

@dataclass(kw_only=True)
class ScriptBase[TSettings:SettingsBase, TClients:ClientsBase](ABC):
    """Execute an imperative operation using configured settings and clients."""
    settings: TSettings
    clients: TClients

    @classmethod
    @abstractmethod
    def get_settings_type(cls) -> type[TSettings]:
        ...

    @classmethod
    @abstractmethod
    def get_clients_type(cls) -> type[TClients]:
        ...

    @classmethod
    def from_environment(cls, argsv=None, **kwargs) -> Self:
        """Construct a script from environment and CLI settings."""
        settings = cls.get_settings_type().from_environment(argsv, **kwargs)
        return cls.from_settings(settings)

    @classmethod
    def from_settings(cls, settings: TSettings) -> Self:
        """Construct a script with initialized clients."""
        clients = cls.get_clients_type().initialize(settings=settings)
        return cls(settings=settings, clients=clients)

    @abstractmethod
    def do(self) -> None:
        """Execute the script."""
        ...