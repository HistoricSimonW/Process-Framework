from abc import ABC, abstractmethod
from collections.abc import Callable, Sized
from dataclasses import dataclass
from operator import eq, ge, gt, le, lt, ne
from typing import Literal, NamedTuple

from process_framework import Step, ValueOrReference, resolve
from ..composition.core import HasInput

type ConditionAction[T] = (
    Callable[[T], None]
    | Step
    | Exception
    | type[Exception]
)


class Evaluated[T](NamedTuple):
    value:T
    result:bool
    
    
@dataclass(kw_only=True)
class ConditionalBase[T](Step, ABC):
    if_true:ConditionAction[T]|None=None
    if_false:ConditionAction[T]|None=None

    @abstractmethod
    def evaluate(self) -> Evaluated[T]:
        ...
        
        
    @staticmethod
    def dispatch_action(evaluated:T, action:ConditionAction[T]|None) -> object:
        """Execute, invoke, or raise the supplied action."""
        if action is None:
            return
        
        if isinstance(action, Step):
            return action.do()
        
        if isinstance(action, Exception):
            raise action
        
        if isinstance(action, type) and issubclass(action, Exception):
            raise action()

        if callable(action):
            return action(evaluated)
        
        raise TypeError(f"Unsupported action: {action!r}")
    
        
    def do(self):
        """Evaluate the condition and dispatch the corresponding action."""
        evaluated, result = self.evaluate()
        
        if result:
            # the true condition
            self.dispatch_action(evaluated, self.if_true)
        else:
            # the false condition
            self.dispatch_action(evaluated, self.if_false)
            

    def preflight(self) -> None:
        """Validate both branches before execution."""
        super().preflight()
        
        if self.if_true is None and self.if_false is None:
            raise ValueError("At least one branch must be configured.")
        
        for action in (self.if_true, self.if_false):
            if isinstance(action, Step):
                action.preflight()
    

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
    """A condition that can be evaluated independently."""
    
    @abstractmethod
    def evaluate(self) -> bool:
        """Evaluate the condition."""
        ...


@dataclass(kw_only=True)
class CountCondition(Condition):
    """Compare the length of a collection against a target."""
    countable:ValueOrReference[Sized]
    target:ValueOrReference[int]
    op:CountOp
    
    def evaluate(self) -> bool:
        count = len(resolve(self.countable))
        return _OPERATORS[self.op](count, resolve(self.target))


@dataclass(kw_only=True)
class IfCondition[T:Condition](ConditionalBase[T]):
    """Dispatch an action based on an independent condition."""
    condition:T
    
    def evaluate(self) -> Evaluated[T]:
        return Evaluated(self.condition, self.condition.evaluate())
    
    
@dataclass(kw_only=True)
class IfPredicate[T](ConditionalBase[T], HasInput[T]):
    """Evaluate a predicate against a resolved input."""
    predicate:Callable[[T], bool]
    
    def evaluate(self) -> Evaluated[T]:
        value = resolve(self.input_)
        return Evaluated(value, self.predicate(value))