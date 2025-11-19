import os
from argparse import ArgumentParser
from pathlib import Path
from abc import ABC
from typing import Sequence, Self
from pydantic import BaseModel, ConfigDict
import logging

class SettingsBase(BaseModel, ABC):

    model_config = ConfigDict(extra="ignore")

    @classmethod
    def __get_env_fns__(cls) -> Sequence[str]:
        return (".env", ".env.local")
    
    @classmethod
    def __add_args__(cls, parser: ArgumentParser) -> None: 
        """ allow subclasses to get args from an argparser; noop by default"""
        # this can be overwritten to add more or different arguments
        # .env
        parser.add_argument('--env-file', type=Path, help='Path to a .env file (required)', default=[], action='append')


    @classmethod
    def __get_args_from_environment__(cls, args: Sequence[str] | None = None) -> dict:
        """ construct a `Settings` object from an .env file, or a sequence of `args` """
        # get args from cli
        try:
            parser = ArgumentParser()
            cls.__add_args__(parser)
            ns, _ = parser.parse_known_args(args)
            cli = {k: v for k, v in vars(ns).items() if v is not None}
        except Exception:
            cli = dict()

        # get args from a .env
        try:
            from dotenv import load_dotenv
            # try .envs specified in the cli, else the defaults
            for fn in cli.get('env_file') or cls.__get_env_fns__():
                logging.info(f'attempting to load .env at {fn}')
                path = Path(fn)
                if path.exists():
                    load_dotenv(path, override=True)
        except ModuleNotFoundError as e:
            if 'dotenv' in str(e):
                logging.warning(f'an `env_file` was parsed from `args`, but module `dotenv` was not found ({e})')
        except Exception:
            ...

        # overwrite environ with cli args
        return dict(os.environ) | cli


    @classmethod
    def from_environment(cls:type[Self], args=None) -> Self:
        args = cls.__get_args_from_environment__(args)
        return cls(**args)
