from process_framework import Step
from random import randrange
from dataclasses import dataclass

@dataclass
class RandomFail(Step):
    step:Step
    exception:type[Exception] = ValueError
    
    min:int = 0
    max:int = 10
    gte:int = 5

    def do(self):
        v = randrange(self.min, self.max)
        if v >= self.gte:
            self.step.do()
        else:
            raise self.exception("OOPS!")