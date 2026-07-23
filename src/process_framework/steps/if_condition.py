from abc import ABC, abstractmethod
from process_framework import Step, IGettable, resolve, ValueOrReference
from dataclasses import dataclass
from itertools import chain
from elasticsearch import Elasticsearch
import logging
from typing import Literal
from operator import eq, ne, lt, le, gt, ge

CountOp = Literal["==", "!=", ">=", ">", "<=", "<"]

_OPERATORS = {
    "==": eq,
    "!=": ne,
    "<": lt,
    "<=": le,
    ">": gt,
    ">=": ge,
}

class Condition(ABC):
    @abstractmethod
    def evaluate(self) -> bool:
        ...

class CountCondition(Condition):
    countable:ValueOrReference
    target:ValueOrReference[int]
    op:CountOp
    
    def evaluate(self):
        count = len(resolve(self.countable))
        return _OPERATORS[self.op](count, resolve(self.target))


@dataclass(kw_only=True)     
class ConditionalStep(Step):
    condition:Condition
    
    if_true:list[Step]|None=None
    if_false:list[Step]|None=None
    
    def preflight(self) -> None:
        super().preflight()
        
        for step in chain(self.if_true or [], self.if_false or []):
            step.preflight()
            
    def do(self):
        
        result = self.condition.evaluate()
        steps = self.if_true if result else self.if_false
        
        if steps is None:
            self._info(f'{type(self.condition).__name__} evaluated to {result} and `if_{str(result).lower()}` is None')
            return
        
        self._info(f'{type(self.condition).__name__} evaluated to {result}; performing {len(steps)} steps')
        for step in steps:
            step.do()
            
