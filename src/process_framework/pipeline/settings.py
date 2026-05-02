from argparse import ArgumentParser
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Any, Self
from pydantic import BaseModel, Field, field_validator


def empty_str_to_none(v:str) -> str|None:
    """ treat an empty string as a None value
        use in, e.g., `Annotated[Optional[int], BeforeValidator(empty_str_none)]` """
    if v == "":
        return None
    return v


class CliMetadata(BaseModel):
    """metadata describing how a settings field is exposed as a cli argument."""
    flags: tuple[str, ...]
    kwargs: dict[str, Any] = Field(default_factory=dict)


def CliArg(*flags: str, **kwargs):
    """mark a settings field as configurable from the cli."""
    meta = CliMetadata(flags=flags, kwargs=kwargs)
    return Field(
        json_schema_extra={ # type:ignore
            "cli": meta.model_dump()
        }
    )


class SettingsBase(BaseSettings):
    """base pipeline settings model loadable from env files and optional cli arguments."""
    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        case_sensitive=False
    )

    @field_validator("*", mode="before")
    @classmethod
    def empty_str_to_none(cls, v, info):
        """coerce empty strings to none for optional fields."""
        field = cls.model_fields.get(info.field_name)

        if v == "" and field:
            anno = field.annotation
            # crude check for Optional
            if "NoneType" in str(anno):
                return None

        return v

    @classmethod
    def add_model_args(cls, parser: ArgumentParser) -> None:
        """add cli arguments for fields marked with CliArg."""
        for name, field in cls.model_fields.items():
            
            extra = field.json_schema_extra
            if not isinstance(extra, dict):
                continue

            cli_raw = extra.get("cli")
            if cli_raw is None:
                continue

            cli = CliMetadata.model_validate(cli_raw)
            
            kwargs = dict(cli.kwargs)
            kwargs.setdefault("dest", name)
            kwargs.setdefault("default", None)

            if "action" not in kwargs and field.annotation in (str, int, float):
                kwargs.setdefault("type", field.annotation)

            parser.add_argument(*cli.flags, **kwargs)


    @classmethod
    def from_environment(cls, argsv=None) -> Self:
        """construct settings from a cli-selected env file."""
        # initialize the parser with default '--env-file' and args from the model
        parser = ArgumentParser()
        parser.add_argument("--env-file", default=None)
        cls.add_model_args(parser)
        
        # parse args
        args, _ = parser.parse_known_args(argsv)
        
        # get args as a dict
        data = vars(args)

        # get env_file
        env_file = data.pop("env_file", None)
        cli_overrides = {k: v for k, v in data.items() if v is not None}
        init_kwargs = {'_env_file':env_file} if env_file else {}

        return cls(**init_kwargs, **cli_overrides)