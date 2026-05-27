from process_framework.steps import AssigningStep
from dataclasses import dataclass
from typing import Callable

@dataclass
class AssignFromFunc[T](AssigningStep[T]):
    func:Callable[[], T]

    def generate_value(self) -> T | None:
        return self.func()