from .step import Step
from ..references.reference import Reference
from abc import abstractmethod, ABC

class AssigningStep[T](Step, ABC):
    """ a step that assigns a typed result to a typed `Reference` """
    def __init__(self, assign_to:Reference[T]):
        super().__init__()
        self.assign_to = assign_to

    @abstractmethod
    def generate(self) -> T:
        pass

    def do(self):
        result = self.generate()
        self.assign_to.set(result)