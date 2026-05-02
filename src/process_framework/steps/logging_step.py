from .step import Step
from ..references.composition.core import IGettable
from ..composition.core import _repr
import logging
from dataclasses import dataclass, KW_ONLY

@dataclass
class Log(Step):
    subject:IGettable
    _ : KW_ONLY
    level:int=logging.INFO

    def do(self):
        
        try:
            idx = self.subject.get_value().index
            message=f'{self.subject!r}, {idx.dtype}, {_repr.repr(list(idx.values))}'

        except:
            message = f'{self.subject!r}'

        self._log(
            level=self.level,
            message=message
        )