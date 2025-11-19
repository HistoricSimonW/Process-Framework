import os
from argparse import ArgumentParser
from pathlib import Path
from abc import ABC
from typing import Sequence, Self, Optional, Annotated, get_origin, get_args, Union, Any
from pydantic import BaseModel, ConfigDict
from pydantic_core import PydanticUndefined
from pydantic.fields import FieldInfo
import logging


def empty_str_to_none(v:str) -> str|None:
    """ treat an empty string as a None value
        use in, e.g., `Annotated[Optional[int], BeforeValidator(empty_str_none)]` """
    if v == "":
        return None
    return v


class SettingsBase(BaseModel, ABC):
    """ base class for Pipeline settings; supports adding arguments to cli argparser """
    model_config = ConfigDict(extra="ignore")

    @classmethod
    def __get_env_fns__(cls) -> Sequence[str]:
        return (".env", ".env.local")
    

    @classmethod
    def __add_model_field_arg__(cls, name:str, field:FieldInfo, parser:ArgumentParser) -> None:
        # skip any private / computed / etc if you have them
        if name.startswith("_"):
            return

        # CLI flag name: e.g. DELETE_CHANGES -> --delete-changes
        flag = "--" + name.lower().replace("_", "-")

        # figure out the underlying type (handle Optional[...] etc.)
        anno = field.annotation
        origin = get_origin(anno)
        args = get_args(anno)

        # type passed to add_argument
        arg_type = None

        if origin is None:
            arg_type = anno
        elif origin is list:
            # example extension: list[str] -> type=str, nargs='*'
            arg_type = args[0] if args else str
        elif origin is Union and type(None) in args:
            # Optional[X]
            non_none = [a for a in args if a is not type(None)]
            arg_type = non_none[0] if non_none else str
        else:
            arg_type = str

        # simple bool handling: --delete-changes sets it to True
        if arg_type is bool:
            parser.add_argument(
                flag,
                dest=name,
                action='store_true',
                default=None,
                help=field.description or f"Set {name} to True",
            )
            return

        # everything else: use `type=` so argparse does conversion
        kwargs:dict[str, Any] = {
            "dest": name,
            "help": field.description or f"Override {name}",
        }

        # only set default if it is NOT PydanticUndefined
        if field.default is not PydanticUndefined:
            kwargs["default"] = field.default

        # set type to int/float/path/str if specified, fall back to str
        kwargs['type'] = arg_type if arg_type in (int, float, str, Path) else str

        parser.add_argument(flag, **kwargs)


    @classmethod
    def __add_args__(cls, parser: ArgumentParser) -> None: 
        """ allow subclasses to get args from an argparser; noop by default"""
        # this can be overwritten to add more or different arguments,
        #   but by default it adds `--env-file` and the 'model_fields' of the 'cls' to the ArgumentParser
        parser.add_argument('--env-file', type=Path, help='Path to a .env file (required)', default=[], action='append')

        for name, field in cls.model_fields.items():
            cls.__add_model_field_arg__(name, field, parser)


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
        """ construct an instance of `cls` from arguments in the environment (args and .env vars) """
        args = cls.__get_args_from_environment__(args)
        return cls(**args)
