from pydantic import BaseModel, ConfigDict
from typing import Self
from abc import ABC, abstractmethod
import os

class CredentialModel(BaseModel, ABC):
    """ base class for `CredentialModel`s which extract arguments from os.environ using a prefix """
    model_config = ConfigDict(extra='ignore')
    
    @classmethod
    def model_from_env(cls:type[Self], prefix:str, **kwargs) -> Self:
        """ construct an instance of a CredentialModel from os.environ, overwriting with any provided kwargs, matching on prefix """
        args = {k.removeprefix(prefix).removeprefix('_'):v for k, v in (dict(os.environ) | kwargs).items() if k.startswith(prefix)}
        return cls(**args)


class ConstructingCredentialModel[TClient](CredentialModel, ABC):
    """ base class for `CredentialModel`s that construct a typed `TClient` """
    
    @classmethod
    @abstractmethod
    def __client_from_kwargs__(cls, **kwargs) -> TClient:
        """ construct an instance of `TClient` from `kwargs`. generally wrap TClient's constructor """
        ...


    def get_client(self) -> TClient:
        """ construct an instance of `TClient` from arguments in `self` """
        return self.__client_from_kwargs__(**{k.lower():v for k, v in self.model_dump(exclude_none=True).items()})


    @classmethod
    def client_from_env(cls, prefix:str, **kwargs) -> TClient:
        """  construct an instance of a `TClient` from os.environ, overwriting with any provided kwargs, matching on prefix """
        model = cls.model_from_env(prefix=prefix, **kwargs)
        return model.get_client()
