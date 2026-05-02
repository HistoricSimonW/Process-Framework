from .step import Step
from .composition.core import HasInput
from ..composition.core import _repr
import logging
from dataclasses import dataclass, KW_ONLY

@dataclass
class LogInput(HasInput, Step):
    """log a representation of the input."""
    _ : KW_ONLY
    level:int=logging.INFO

    def do(self) -> None:
        """log input value with optional index metadata (for pandas-like)."""
        try:
            idx = self.input_.get_value().index
            message=f'{self.input_!r}, {idx.dtype}, {_repr.repr(list(idx.values))}'

        except Exception:
            message = f'{self.input_!r}'

        self._log(
            level=self.level,
            message=message
        )


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