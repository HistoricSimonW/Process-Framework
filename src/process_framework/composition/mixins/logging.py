from dataclasses import dataclass
from typing import Any
import logging

@dataclass
class HasLogger:
    """provides class-based logging helpers using the class name as logger."""
    @classmethod
    def _log(cls, level: int, message: str, *args: Any) -> None:
        """ log a `message` at the provided `level` """
        logging.getLogger(cls.__qualname__).log(
            level,
            message,
            *args,
        )
        
    @classmethod
    def _info(cls, message: str, *args: Any) -> None:
        """ log an `info` message """
        cls._log(logging.INFO, message, *args)

    @classmethod
    def _warn(cls, message: str, *args: Any) -> None:
        """ log a `warning` message` """
        cls._log(logging.WARNING, message, *args)

    @classmethod
    def _error(cls, message: str, *args: Any) -> None:
        """ log an `error` message """
        cls._log(logging.ERROR, message, *args)