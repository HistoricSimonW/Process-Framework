import os
from argparse import ArgumentParser
from pathlib import Path
from abc import ABC
from typing import Sequence, Self
from pydantic import BaseModel, ConfigDict


class SettingsBase(BaseModel, ABC):

    model_config = ConfigDict(extra="ignore")

    @classmethod
    def __get_env_fns__(cls) -> Sequence[str]:
        return (".env", ".env.local")


    @classmethod
    def __add_args__(cls, parser: ArgumentParser) -> None: 
        """ allow subclasses to get args from an argparser; noop by default"""
        ...


    @classmethod
    def __get_args_from_environment__(cls, args: Sequence[str] | None = None) -> dict:
        # get args from a .env
        try:
            from dotenv import load_dotenv

            for fn in cls.__get_env_fns__():
                path = Path(fn)
                if path.exists():
                    load_dotenv(path, override=True)
        except Exception:
            ...

        # get args from cli
        try:
            parser = ArgumentParser()
            cls.__add_args__(parser)
            ns, _ = parser.parse_known_args(args)
            cli = {k: v for k, v in vars(ns).items() if v is not None}
        except Exception:
            cli = dict()

        # overwrite environ with cli args
        return dict(os.environ) | cli


    @classmethod
    def from_environment(cls:type[Self], args=None) -> Self:
        args = cls.__get_args_from_environment__(args)
        return cls(**args)
