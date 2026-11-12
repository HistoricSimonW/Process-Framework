from .step import Step
from .composition.core import HasInput
from ..composition.core import _repr
from ..references import resolve
import logging
from dataclasses import dataclass, KW_ONLY, field
from typing import Callable, Any, Sequence

type InputFormatter = Callable[[Any], Any]

@dataclass(kw_only=True)
class LogInput(HasInput, Step):
    """log a representation of the input."""
    
    level:int=logging.INFO
    format:str|None = None
    args:Sequence[InputFormatter] = ()
    kwargs:dict[str, InputFormatter] = field(default_factory=dict)

    def do(self) -> None:
        """Evaluate formatting functions and log their results."""
        value = resolve(self.input_)
        
        # evaluate args
        args = [func(value) for func in self.args]
        
        # evaluate kwargs
        kwargs = {
            key: func(value)
            for key, func in self.kwargs.items()
        }

        # try to build a message using format,
        if self.format is not None:
            message = self.format.format(*args, **kwargs)
        
        # else build a message from available arguments
        elif args or kwargs:
            message = ", ".join([
                *(repr(value) for value in args),
                *(f"{key}: {value!r}" for key, value in kwargs.items()),
            ])
        
        # else a representation of the input
        else:
            message = _repr.repr(self.input_)

        self._log(level=self.level, message=message)


@dataclass
class LogMessage(Step):
    """log a static message."""
    message:str
    _ :KW_ONLY
    level:int=logging.INFO

    def do(self) -> None:
        """emit the configured log message."""
        self._log(
            level=self.level, 
            message=self.message
        )