# Standard library
from dataclasses import dataclass
from typing import Callable

# Process framework
from process_framework import Step, ValueOrReference, resolve
from process_framework.steps.composition.core import HasInput


type ConditionAction[T] = (
    Callable[[T], None]
    | Step
    | Exception
    | type[Exception]
)
  
  
@dataclass(kw_only=True)
class IfThen[T](Step):
    input_:ValueOrReference[T]
    condition:Callable[[T], bool]
    if_true:ConditionAction[T]|None=None
    if_false:ConditionAction[T]|None=None
    
    @staticmethod
    def dispatch_action(input_:T, action:ConditionAction[T]|None) -> None:
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
            return action(input_)
        
        raise TypeError(f"Unsupported action: {action!r}")
    
    
    def do(self):
        """Evaluate the condition and dispatch the corresponding action."""
        input_ = resolve(self.input_)
        if self.condition(input_):
            # the true condition
            self.dispatch_action(input_, self.if_true)
        else:
            # the false condition
            self.dispatch_action(input_, self.if_false)
            

    def preflight(self) -> None:
        """Validate both branches before execution."""
        if self.if_true is None and self.if_false is None:
            raise ValueError()
        
        for action in (self.if_true, self.if_false):
            if isinstance(action, Step):
                action.preflight()
          