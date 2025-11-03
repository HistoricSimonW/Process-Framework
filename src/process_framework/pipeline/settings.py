# stdlib
import os
from argparse import ArgumentParser
from pathlib import Path
from abc import ABC
from typing import TypeVar, Sequence
from pydantic import BaseModel, ConfigDict

TSettings = TypeVar("TSettings", bound="SettingsBase")


class SettingsBase(BaseModel, ABC):

    model_config = ConfigDict(extra="ignore")

    @classmethod
    def __get_env_fns__(cls) -> Sequence[str]:
        return (".env", ".env.local")

    @classmethod
    def __add_args__(cls, parser: ArgumentParser) -> None: ...

    @classmethod
    def __get_args_from_environment__(cls, args: Sequence[str] | None = None) -> dict:
        # get args from a .env
        try:
            from dotenv import load_dotenv

            for fn in cls.__get_env_fns__():
                load_dotenv(Path(fn), override=True)
        except:
            ...

        # get args from cli
        try:
            parser = ArgumentParser()
            cls.__add_args__(parser)
            cli = parser.parse_args(args).__dict__
        except:
            cli = dict()

        # cli overwrites environ
        return os.environ | cli

    @classmethod
    def from_environment(cls, args=None):
        args = cls.__get_args_from_environment__(args)
        return cls(**args)
