from .step import Step
from ..references.reference import Reference
from abc import abstractmethod, ABC

class AssigningStep[T](Step, ABC):
    """ a step that assigns a typed result to a typed `Reference` """
    def __init__(self, assign_to:Reference[T], *, overwrite:bool=True):
        super().__init__()
        self.assign_to = assign_to
        self.overwrite=overwrite

    @abstractmethod
    def generate(self) -> T:
        pass

    def do(self):
        if self.assign_to.has_value() and not self.overwrite:
            return self.assign_to.get_value()
        
        result = self.generate()
        self.assign_to.set(result)