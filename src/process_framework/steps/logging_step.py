from .step import Step
from ..references import Reference
from .step import Step
from ..references import Reference, _repr
import logging

class Log(Step):
    def __init__(self, subject:Reference, level:int = logging.INFO) -> None:
        super().__init__()
        self.subject = subject
        self.level = level


    def log(self, msg:str):
        logging.log(self.level, msg)


    def do(self):
        try:
            idx = self.subject.get_value().index
            self.log(f'{self.subject}, {idx.dtype}, {_repr.repr(list(idx.values))}')
        except:
            self.log(f'{self.subject}')